"""
fraud_etl_dag.py
═══════════════════════════════════════════════════════════════════
FinTech Fraud Detection Pipeline — Airflow DAG

Schedule: every 6 hours  (0 */6 * * *)

Tasks:
  1. extract_validated_transactions
       Pull non-fraud transactions from PostgreSQL for the last 6-hour window.

  2. write_parquet_warehouse
       Persist validated records to Parquet files (data warehouse layer).

  3. generate_reconciliation_report
       Compare total ingress amount vs validated amount.
       Write report row to reconciliation_reports table.
       Save a human-readable CSV report to /opt/airflow/data/reports/.

  4. generate_fraud_merchant_report
       Aggregate fraud_alerts by merchant_category.
       Save analytic CSV → reports directory.
       Upsert summary into fraud_by_merchant table.
═══════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Config ────────────────────────────────────────────────────
PG_CONN = {
    "host":     "fraud_postgres",
    "port":     5432,
    "dbname":   "fraud_detection",
    "user":     "fraud_admin",
    "password": "fraud_secure_pass",
}
VALIDATED_DATA_PATH = Path(os.getenv("VALIDATED_DATA_PATH", "/opt/airflow/data/validated"))
REPORTS_PATH        = Path(os.getenv("REPORTS_PATH",        "/opt/airflow/data/reports"))
WINDOW_HOURS        = 6   # matches DAG schedule interval

log = logging.getLogger("fraud_etl_dag")


# ══════════════════════════════════════════════════════════════
#  Helper: get window boundaries from Airflow context
# ══════════════════════════════════════════════════════════════
def _get_window(context) -> tuple[datetime, datetime]:
    """Return (window_start, window_end) for the current DAG run."""
    window_end   = context["data_interval_end"]
    window_start = window_end - timedelta(hours=WINDOW_HOURS)
    return window_start, window_end


# ══════════════════════════════════════════════════════════════
#  Task 1: Extract validated transactions from PostgreSQL
# ══════════════════════════════════════════════════════════════
def extract_validated_transactions(**context):
    window_start, window_end = _get_window(context)

    log.info("🔍  Extracting validated transactions: %s → %s", window_start, window_end)

    conn  = psycopg2.connect(**PG_CONN)
    query = """
        SELECT
            user_id,
            event_timestamp,
            merchant_category,
            amount,
            location_country,
            location_city,
            is_fraud,
            fraud_reason
        FROM transactions
        WHERE event_timestamp >= %(start)s
          AND event_timestamp <  %(end)s
        ORDER BY event_timestamp
    """
    df = pd.read_sql(query, conn, params={"start": window_start, "end": window_end})
    conn.close()

    log.info(
        "📊  Fetched %d total rows  |  fraud=%d  |  valid=%d",
        len(df),
        df["is_fraud"].sum(),
        (~df["is_fraud"]).sum(),
    )

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="total_count",         value=int(len(df)))
    context["ti"].xcom_push(key="total_amount",        value=float(df["amount"].sum()))
    context["ti"].xcom_push(key="fraud_count",         value=int(df["is_fraud"].sum()))
    context["ti"].xcom_push(key="fraud_amount",        value=float(df[df["is_fraud"]]["amount"].sum()))
    context["ti"].xcom_push(key="window_start",        value=window_start.isoformat())
    context["ti"].xcom_push(key="window_end",          value=window_end.isoformat())

    # Save validated (non-fraud) rows to XCom as JSON for Parquet task
    validated_df = df[~df["is_fraud"]].copy()
    context["ti"].xcom_push(key="validated_count",     value=int(len(validated_df)))
    context["ti"].xcom_push(key="validated_amount",    value=float(validated_df["amount"].sum()))
    context["ti"].xcom_push(key="validated_json",      value=validated_df.to_json(date_format="iso"))

    log.info("✅  Extraction complete.")


# ══════════════════════════════════════════════════════════════
#  Task 2: Write validated data to Parquet (data warehouse)
# ══════════════════════════════════════════════════════════════
def write_parquet_warehouse(**context):
    ti             = context["ti"]
    validated_json = ti.xcom_pull(key="validated_json",  task_ids="extract_validated_transactions")
    window_start   = ti.xcom_pull(key="window_start",    task_ids="extract_validated_transactions")

    if not validated_json:
        log.warning("⚠️   No validated data to write for this window.")
        return

    validated_df = pd.read_json(validated_json)

    # Partition by date/hour
    window_dt   = datetime.fromisoformat(window_start)
    parquet_dir = VALIDATED_DATA_PATH / f"year={window_dt.year}" / f"month={window_dt.month:02d}" / f"day={window_dt.day:02d}"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    parquet_file = parquet_dir / f"validated_{window_dt.strftime('%H%M%S')}.parquet"
    table        = pa.Table.from_pandas(validated_df)
    pq.write_table(table, str(parquet_file), compression="snappy")

    log.info("💾  Parquet written → %s  (%d rows)", parquet_file, len(validated_df))
    context["ti"].xcom_push(key="parquet_path", value=str(parquet_file))


# ══════════════════════════════════════════════════════════════
#  Task 3: Generate reconciliation report
# ══════════════════════════════════════════════════════════════
def generate_reconciliation_report(**context):
    ti = context["ti"]

    total_count      = ti.xcom_pull(key="total_count",      task_ids="extract_validated_transactions")
    total_amount     = ti.xcom_pull(key="total_amount",     task_ids="extract_validated_transactions")
    fraud_count      = ti.xcom_pull(key="fraud_count",      task_ids="extract_validated_transactions")
    fraud_amount     = ti.xcom_pull(key="fraud_amount",     task_ids="extract_validated_transactions")
    validated_count  = ti.xcom_pull(key="validated_count",  task_ids="extract_validated_transactions")
    validated_amount = ti.xcom_pull(key="validated_amount", task_ids="extract_validated_transactions")
    window_start     = ti.xcom_pull(key="window_start",     task_ids="extract_validated_transactions")
    window_end       = ti.xcom_pull(key="window_end",       task_ids="extract_validated_transactions")

    fraud_rate = round((fraud_count / total_count * 100), 2) if total_count > 0 else 0.0
    discrepancy = total_amount - validated_amount - fraud_amount

    # ── Log human-readable summary ─────────────────────────
    separator = "─" * 55
    log.info(separator)
    log.info("  📋  RECONCILIATION REPORT")
    log.info("  Window : %s → %s", window_start, window_end)
    log.info(separator)
    log.info("  Total transactions ingressed : %6d   $%12.2f", total_count,     total_amount)
    log.info("  Validated (non-fraud)        : %6d   $%12.2f", validated_count, validated_amount)
    log.info("  Flagged as FRAUD             : %6d   $%12.2f", fraud_count,     fraud_amount)
    log.info("  Fraud rate                   :  %5.2f%%",       fraud_rate)
    log.info("  Discrepancy                  :          $%12.2f", discrepancy)
    log.info(separator)

    # ── Write to PostgreSQL ───────────────────────────────
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()
    cur.execute(
        """
        INSERT INTO reconciliation_reports
            (report_window_start, report_window_end,
             total_transactions, total_ingress_amount,
             fraud_count, fraud_amount,
             validated_count, validated_amount,
             fraud_rate_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            window_start, window_end,
            total_count, total_amount,
            fraud_count, fraud_amount,
            validated_count, validated_amount,
            fraud_rate,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()

    # ── Save CSV report ───────────────────────────────────
    REPORTS_PATH.mkdir(parents=True, exist_ok=True)
    window_dt   = datetime.fromisoformat(window_start)
    report_file = REPORTS_PATH / f"reconciliation_{window_dt.strftime('%Y%m%d_%H%M%S')}.csv"

    with open(report_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Field", "Value"])
        writer.writerow(["Report Window Start",           window_start])
        writer.writerow(["Report Window End",             window_end])
        writer.writerow(["Total Transactions Ingressed",  total_count])
        writer.writerow(["Total Ingress Amount ($)",      f"{total_amount:.2f}"])
        writer.writerow(["Validated Count",               validated_count])
        writer.writerow(["Validated Amount ($)",          f"{validated_amount:.2f}"])
        writer.writerow(["Fraud Count",                   fraud_count])
        writer.writerow(["Fraud Amount ($)",              f"{fraud_amount:.2f}"])
        writer.writerow(["Fraud Rate (%)",                f"{fraud_rate:.2f}"])
        writer.writerow(["Discrepancy ($)",               f"{discrepancy:.2f}"])

    log.info("📄  Reconciliation CSV saved → %s", report_file)


# ══════════════════════════════════════════════════════════════
#  Task 4: Fraud by merchant category — analytic report
# ══════════════════════════════════════════════════════════════
def generate_fraud_merchant_report(**context):
    window_start_str = context["ti"].xcom_pull(
        key="window_start", task_ids="extract_validated_transactions"
    )
    window_start = datetime.fromisoformat(window_start_str)
    report_date  = window_start.date()

    conn  = psycopg2.connect(**PG_CONN)
    query = """
        SELECT
            merchant_category,
            COUNT(*)          AS fraud_count,
            SUM(amount)       AS total_fraud_amount
        FROM fraud_alerts
        WHERE DATE(alert_generated_at) = %(report_date)s
        GROUP BY merchant_category
        ORDER BY fraud_count DESC
    """
    df = pd.read_sql(query, conn, params={"report_date": report_date})
    conn.close()

    if df.empty:
        log.info("ℹ️   No fraud alerts for %s — skipping merchant report.", report_date)
        return

    # ── Log summary table ─────────────────────────────────
    log.info("─" * 55)
    log.info("  🔍  FRAUD BY MERCHANT CATEGORY — %s", report_date)
    log.info("  %-20s  %6s  %14s", "Category", "Count", "Amount ($)")
    log.info("  " + "─" * 44)
    for _, row in df.iterrows():
        log.info(
            "  %-20s  %6d  %14.2f",
            row["merchant_category"], row["fraud_count"], row["total_fraud_amount"],
        )
    log.info("─" * 55)

    # ── Upsert into PostgreSQL ────────────────────────────
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO fraud_by_merchant
                (report_date, merchant_category, fraud_count, total_fraud_amount)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (report_date, merchant_category)
            DO UPDATE SET
                fraud_count        = EXCLUDED.fraud_count,
                total_fraud_amount = EXCLUDED.total_fraud_amount
            """,
            (report_date, row["merchant_category"], int(row["fraud_count"]), float(row["total_fraud_amount"])),
        )
    conn.commit()
    cur.close()
    conn.close()

    # ── Save analytic CSV ─────────────────────────────────
    REPORTS_PATH.mkdir(parents=True, exist_ok=True)
    report_file = REPORTS_PATH / f"fraud_by_merchant_{report_date}.csv"
    df.to_csv(report_file, index=False)
    log.info("📄  Fraud-by-merchant CSV saved → %s", report_file)


# ══════════════════════════════════════════════════════════════
#  DAG definition
# ══════════════════════════════════════════════════════════════
DEFAULT_ARGS = {
    "owner":           "fraud_team",
    "start_date":      datetime(2025, 1, 1, tzinfo=timezone.utc),
    "retries":         2,
    "retry_delay":     timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="fraud_etl_pipeline",
    description="Every-6h ETL: validated Parquet + reconciliation + fraud analytics",
    schedule_interval="0 */6 * * *",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["fintech", "fraud", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_validated_transactions",
        python_callable=extract_validated_transactions,
    )

    t2 = PythonOperator(
        task_id="write_parquet_warehouse",
        python_callable=write_parquet_warehouse,
    )

    t3 = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=generate_reconciliation_report,
    )

    t4 = PythonOperator(
        task_id="generate_fraud_merchant_report",
        python_callable=generate_fraud_merchant_report,
    )

    # ── Task dependencies ─────────────────────────────────
    # Extract → Parquet + Reconciliation → Fraud Report
    t1 >> [t2, t3] >> t4
