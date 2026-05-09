"""
transaction_producer.py
═══════════════════════════════════════════════════════════════════
FinTech Fraud Detection Pipeline — Kafka Transaction Producer

Generates synthetic credit-card transactions and publishes them to
the Kafka topic 'transactions_topic'.

Fraud injection strategy (controlled, ~10% of events):
  1. High-value spike   — amount > $5,000 (random merchant, any country)
  2. Impossible travel  — same user_id, two different countries within
                          a short window (simulated by emitting two
                          back-to-back messages with different countries)

Run: python transaction_producer.py
═══════════════════════════════════════════════════════════════════
"""

import json
import random
import time
import uuid
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("transaction_producer")

# ── Kafka config ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
TRANSACTION_TOPIC       = "transactions_topic"
PAUSE_INTERVAL_SEC      = 2       # seconds between normal transactions
FRAUD_INJECT_EVERY      = 10      # inject 1 fraud event per N normal events
STREAMING_DURATION_SEC  = 3600    # run for 1 hour (0 = forever)

# ── Reference data ───────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "restaurant",
    "online_retail", "entertainment", "fuel", "healthcare",
    "luxury", "atm_cash",
]

# Normal user pool — 20 users generate legitimate traffic
USER_IDS = [f"USR_{i:04d}" for i in range(1, 21)]

# Location catalogue: country → list of cities
LOCATIONS = {
    "Sri Lanka":      ["Colombo", "Kandy", "Galle", "Negombo"],
    "India":          ["Mumbai", "Delhi", "Bangalore", "Chennai"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Edinburgh"],
    "United States":  ["New York", "Los Angeles", "Chicago", "Houston"],
    "Germany":        ["Berlin", "Munich", "Hamburg", "Frankfurt"],
    "Singapore":      ["Singapore"],
    "UAE":            ["Dubai", "Abu Dhabi", "Sharjah"],
    "Australia":      ["Sydney", "Melbourne", "Brisbane", "Perth"],
    "Japan":          ["Tokyo", "Osaka", "Kyoto", "Nagoya"],
    "France":         ["Paris", "Lyon", "Marseille", "Nice"],
}

NORMAL_COUNTRIES  = ["Sri Lanka", "India", "United Kingdom"]
FOREIGN_COUNTRIES = [c for c in LOCATIONS if c not in NORMAL_COUNTRIES]

# ── Helpers ───────────────────────────────────────────────────
def random_location(country=None):
    """Return (country, city). If country is None, pick a normal country."""
    if country is None:
        country = random.choice(NORMAL_COUNTRIES)
    city = random.choice(LOCATIONS[country])
    return country, city


def build_transaction(user_id=None, country=None, amount=None, category=None):
    """Construct a single transaction dict."""
    if user_id  is None: user_id  = random.choice(USER_IDS)
    if category is None: category = random.choice(MERCHANT_CATEGORIES)
    if amount   is None: amount   = round(random.uniform(5.0, 1500.0), 2)

    loc_country, loc_city = random_location(country)

    return {
        "user_id":           user_id,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "merchant_category": category,
        "amount":            amount,
        "location_country":  loc_country,
        "location_city":     loc_city,
        "transaction_id":    str(uuid.uuid4()),
    }


# ── Fraud injection functions ─────────────────────────────────
def inject_high_value_transaction():
    """Generate a single transaction with amount > $5,000 (rule #2)."""
    amount   = round(random.uniform(5001.0, 25000.0), 2)
    category = random.choice(["luxury", "electronics", "atm_cash", "travel"])
    country  = random.choice(FOREIGN_COUNTRIES)
    txn      = build_transaction(amount=amount, category=category, country=country)
    log.warning(
        "🚨 FRAUD INJECT [HIGH-VALUE] user=%s amount=$%.2f country=%s",
        txn["user_id"], txn["amount"], txn["location_country"],
    )
    return [txn]


def inject_impossible_travel():
    """
    Generate TWO transactions for the same user from different countries
    within the same second — impossible travel (rule #1).
    """
    user_id    = random.choice(USER_IDS)
    country_a  = random.choice(NORMAL_COUNTRIES)
    # Pick a foreign country that is different from country_a
    country_b  = random.choice([c for c in FOREIGN_COUNTRIES if c != country_a])
    amount_a   = round(random.uniform(50.0, 800.0), 2)
    amount_b   = round(random.uniform(50.0, 800.0), 2)

    txn_a = build_transaction(user_id=user_id, country=country_a, amount=amount_a)
    txn_b = build_transaction(user_id=user_id, country=country_b, amount=amount_b)

    log.warning(
        "🚨 FRAUD INJECT [IMPOSSIBLE-TRAVEL] user=%s  %s → %s",
        user_id, country_a, country_b,
    )
    return [txn_a, txn_b]


# ── Kafka delivery callback ───────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        log.error("❌  Delivery failed: %s", err)
    else:
        log.info(
            "✅  Delivered to %s [partition %d] offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── Producer setup ────────────────────────────────────────────
def create_producer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id":         "fraud_transaction_producer",
        "acks":              "all",          # wait for all replicas
        "retries":           5,
        "retry.backoff.ms":  300,
    }
    return Producer(conf)


def publish(producer, topic, payload):
    """Serialise and publish a single transaction dict."""
    producer.produce(
        topic=topic,
        key=payload["user_id"].encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)   # trigger callbacks without blocking


# ── Main streaming loop ───────────────────────────────────────
def run():
    producer      = create_producer()
    start_time    = time.time()
    event_counter = 0

    log.info("=" * 60)
    log.info("  FinTech Fraud Detection — Transaction Producer")
    log.info("  Topic  : %s", TRANSACTION_TOPIC)
    log.info("  Brokers: %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("  Fraud injection every %d normal events", FRAUD_INJECT_EVERY)
    log.info("=" * 60)

    try:
        while True:
            # ── Check runtime limit ──────────────────────────
            if STREAMING_DURATION_SEC > 0:
                if time.time() - start_time > STREAMING_DURATION_SEC:
                    log.info("⏹  Reached streaming duration limit (%ds). Stopping.", STREAMING_DURATION_SEC)
                    break

            event_counter += 1

            # ── Decide: normal or fraud? ─────────────────────
            if event_counter % FRAUD_INJECT_EVERY == 0:
                # Alternate between the two fraud types
                if (event_counter // FRAUD_INJECT_EVERY) % 2 == 0:
                    transactions = inject_high_value_transaction()
                else:
                    transactions = inject_impossible_travel()
            else:
                transactions = [build_transaction()]

            # ── Publish all transactions ─────────────────────
            for txn in transactions:
                log.info(
                    "📤  [Event #%04d] user=%-10s cat=%-14s amount=$%8.2f  %s / %s",
                    event_counter,
                    txn["user_id"],
                    txn["merchant_category"],
                    txn["amount"],
                    txn["location_city"],
                    txn["location_country"],
                )
                publish(producer, TRANSACTION_TOPIC, txn)

            time.sleep(PAUSE_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("🛑  Producer interrupted by user.")
    finally:
        log.info("🔄  Flushing remaining messages …")
        producer.flush(timeout=10)
        log.info("✅  Producer shut down cleanly after %d events.", event_counter)


if __name__ == "__main__":
    run()
