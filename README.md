# FinTech Fraud Detection Pipeline

**EC8207 Applied Big Data Engineering — Mini Project**  
Scenario 2: FinTech Fraud Detection | Lambda Architecture

---

## Architecture Overview

```
API/Simulator ─► Python Producer ─► Kafka (3 brokers)
                                        │
                      ┌─────────────────┘
                      ▼
               Apache Spark (Structured Streaming)
               ├── Rule 1: Impossible Travel (10-min window)
               └── Rule 2: High-Value > $5,000
                      │
          ┌───────────┴────────────┐
          ▼                        ▼
   fraud_alerts table        transactions table
   (PostgreSQL)               (PostgreSQL, all rows)
                                   │
                    ┌──────────────┘
                    ▼
             Apache Airflow (every 6 hours)
             ├── Extract validated transactions
             ├── Write Parquet (data warehouse)
             ├── Reconciliation Report (CSV + DB)
             └── Fraud-by-Merchant Report (CSV + DB)
```

---

## Services & Ports

| Service           | Port  | URL / Notes                         |
|-------------------|-------|-------------------------------------|
| Kafka UI          | 8888  | http://localhost:8888               |
| Airflow UI        | 8082  | http://localhost:8082 (admin/admin) |
| Spark Master UI   | 9090  | http://localhost:9090               |
| PostgreSQL (fraud)| 5433  | fraud_admin / fraud_secure_pass     |
| Kafka Broker 1    | 9092  | External listener                   |
| Kafka Broker 2    | 9093  | External listener                   |
| Kafka Broker 3    | 9094  | External listener                   |
| Schema Registry   | 8081  |                                     |

---

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24.0 with Docker Compose v2
- 8 GB RAM allocated to Docker (recommended)
- Ports 8081, 8082, 8888, 9090, 9092–9094, 5433 free

### 1. Start everything
```bash
chmod +x run.sh
./run.sh up
```
This will:
- Create the `docker_fraud` network
- Start all Docker containers
- Wait for services to stabilise
- Create Kafka topics (`transactions_topic`, `fraud_alerts_topic`)
- Download Spark JARs
- Deploy the Spark job into the container

### 2. Start the transaction producer
In a new terminal:
```bash
./run.sh producer
```
You will see clean logs like:
```
2025-01-01 10:00:02 [PRODUCER] INFO — 📤  [Event #0001] user=USR_0007  cat=grocery        amount=$ 234.50  Colombo / Sri Lanka
2025-01-01 10:00:12 [PRODUCER] WARNING — 🚨 FRAUD INJECT [HIGH-VALUE] user=USR_0003 amount=$12450.00 country=United States
```

### 3. Submit the Spark fraud detection job
In another terminal:
```bash
./run.sh spark
```

### 4. Verify data in PostgreSQL
```bash
docker exec -it fraud_postgres psql -U fraud_admin -d fraud_detection

-- Check transactions
SELECT is_fraud, COUNT(*), SUM(amount) FROM transactions GROUP BY is_fraud;

-- Check fraud alerts
SELECT fraud_reason, COUNT(*) FROM fraud_alerts GROUP BY fraud_reason;

-- Check reports
SELECT * FROM reconciliation_reports ORDER BY generated_at DESC LIMIT 5;
```

### 5. Trigger Airflow DAG manually
- Open http://localhost:8082 (admin / admin)
- Enable the `fraud_etl_pipeline` DAG
- Click "Trigger DAG" to run immediately
- Reports appear in `./reports/`

---

## Project Files

```
fintech-fraud-detection/
├── docker-compose.yml          # All services
├── .env                        # Environment variables
├── run.sh                      # One-command setup & run
├── requirements.txt            # Python dependencies
│
├── producer/
│   └── transaction_producer.py # Kafka producer (data simulator)
│
├── spark/
│   └── spark_fraud_detection.py # Spark Structured Streaming job
│
├── dags/
│   └── fraud_etl_dag.py        # Airflow DAG (6-hourly ETL)
│
├── postgres-init/
│   └── 01_init_schema.sql      # Auto-run DB schema creation
│
└── reports/                    # Generated CSV reports (mounted volume)
```

---

## Fraud Detection Rules

### Rule 1 — Impossible Travel
Within a **10-minute tumbling window**, if the same `user_id` appears
in **two or more distinct countries**, the record is flagged as fraud.

The producer simulates this by emitting two transactions for the same
user with different countries in rapid succession (every 10th event
triggers an impossible-travel pair).

### Rule 2 — High-Value Transaction
Any single transaction with `amount > $5,000` is immediately flagged.

The producer injects a high-value transaction to a foreign merchant
every 20th event.

---

## Output Reports

### Reconciliation Report (every 6h)
Saved to `reports/reconciliation_YYYYMMDD_HHMMSS.csv`:

| Field                       | Value       |
|-----------------------------|-------------|
| Report Window Start         | 2025-01-01T00:00:00 |
| Total Transactions Ingressed| 1,200       |
| Total Ingress Amount ($)    | 245,600.00  |
| Validated Amount ($)        | 198,400.00  |
| Fraud Count                 | 85          |
| Fraud Rate (%)              | 7.08        |

### Fraud by Merchant Category (daily)
Saved to `reports/fraud_by_merchant_YYYY-MM-DD.csv`:

| merchant_category | fraud_count | total_fraud_amount |
|-------------------|-------------|--------------------|
| luxury            | 24          | 312,450.00         |
| atm_cash          | 18          | 184,200.00         |
| travel            | 15          | 97,350.00          |

---

## Stopping the Pipeline
```bash
./run.sh down
```
