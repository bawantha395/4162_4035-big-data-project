#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
#  FinTech Fraud Detection Pipeline — Setup & Run Script
#  Usage:  chmod +x run.sh && ./run.sh [command]
#  Commands: up | down | producer | spark | logs | reset
# ═══════════════════════════════════════════════════════════════

set -euo pipefail

COMPOSE="docker compose"
SPARK_JARS_DIR="spark/jars"
SPARK_VERSION="3.5.1"
KAFKA_VERSION="2.8.1"
POSTGRES_JDBC="42.7.1"

print_banner() {
  echo ""
  echo "╔══════════════════════════════════════════════════════════╗"
  echo "║   FinTech Fraud Detection Pipeline                      ║"
  echo "║   Kafka · Spark · Airflow · PostgreSQL · Docker         ║"
  echo "╚══════════════════════════════════════════════════════════╝"
  echo ""
}

# ── Step 1: Create Docker network ─────────────────────────────
setup_network() {
  echo "🌐  Setting up Docker network …"
  docker network create docker_fraud 2>/dev/null || echo "   Network already exists — skipping."
}

# ── Step 2: Start all services ────────────────────────────────
start_services() {
  echo "🚀  Starting all services …"
  $COMPOSE -f docker-compose.yml up -d
  echo ""
  echo "⏳  Waiting 60 seconds for services to stabilise …"
  sleep 60
}

# ── Step 3: Create Kafka topics ───────────────────────────────
create_kafka_topics() {
  echo "📌  Creating Kafka topics …"

  docker exec kafka_broker_1 kafka-topics \
    --create --if-not-exists \
    --bootstrap-server kafka_broker_1:19092 \
    --topic transactions_topic \
    --partitions 3 \
    --replication-factor 3

  docker exec kafka_broker_1 kafka-topics \
    --create --if-not-exists \
    --bootstrap-server kafka_broker_1:19092 \
    --topic fraud_alerts_topic \
    --partitions 3 \
    --replication-factor 3

  echo "✅  Topics created:"
  docker exec kafka_broker_1 kafka-topics \
    --list --bootstrap-server kafka_broker_1:19092
}

# ── Step 4: Download Spark JARs ───────────────────────────────
download_spark_jars() {
  echo "📦  Downloading Spark JARs …"
  mkdir -p "$SPARK_JARS_DIR"

  # Kafka connector for Spark
  curl -fsSL -o "$SPARK_JARS_DIR/kafka-clients-${KAFKA_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar"

  curl -fsSL -o "$SPARK_JARS_DIR/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"

  # PostgreSQL JDBC driver
  curl -fsSL -o "$SPARK_JARS_DIR/postgresql-${POSTGRES_JDBC}.jar" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC}/postgresql-${POSTGRES_JDBC}.jar"

  # Commons pool (required by Spark-Kafka)
  curl -fsSL -o "$SPARK_JARS_DIR/commons-pool2-2.11.1.jar" \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

  # Spark token provider (required by Spark 3.4 Kafka integration)
  curl -fsSL -o "$SPARK_JARS_DIR/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"

  echo "✅  JARs downloaded to $SPARK_JARS_DIR/"
  ls -lh "$SPARK_JARS_DIR/"
}

# ── Step 5: Copy Spark job into container ─────────────────────
deploy_spark_job() {
  echo "📤  Copying Spark job into spark_master container …"
  docker exec spark_master mkdir -p /opt/spark/jobs /opt/spark/jars
  docker cp spark/spark_fraud_detection.py spark_master:/opt/spark/jobs/
  for jar in spark/jars/*.jar; do
    docker cp "$jar" spark_master:/opt/spark/jars/
  done
  echo "✅  Spark job deployed."
}

# ── Run: Producer ─────────────────────────────────────────────
run_producer() {
  echo "📤  Starting transaction producer (inside airflow_webserver) …"
  echo "     Press Ctrl+C to stop."
  docker exec -it airflow_webserver python /opt/airflow/spark/../producer/transaction_producer.py
}

# ── Run: Spark fraud detection ────────────────────────────────
run_spark() {
  JAR_LIST=$(ls spark/jars/*.jar | xargs -I{} docker exec spark_master ls /opt/spark/jars/ | \
    grep "\.jar$" | sed 's|^|/opt/spark/jars/|' | tr '\n' ',' | sed 's/,$//')

  echo "⚡  Submitting Spark fraud detection job …"
  docker exec spark_master spark-submit \
    --master local[2] \
    --jars "/opt/spark/jars/kafka-clients-${KAFKA_VERSION}.jar,\
  /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar,\
  /opt/spark/jars/postgresql-${POSTGRES_JDBC}.jar,\
  /opt/spark/jars/commons-pool2-2.11.1.jar,\
  /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
    /opt/spark/jobs/spark_fraud_detection.py
}

# ── Show service URLs ─────────────────────────────────────────
show_urls() {
  echo ""
  echo "┌─────────────────────────────────────────────────────────┐"
  echo "│  Service URLs                                           │"
  echo "├─────────────────────────────────────────────────────────┤"
  echo "│  Kafka UI      → http://localhost:8888                  │"
  echo "│  Airflow UI    → http://localhost:8082  (admin/admin)   │"
  echo "│  Spark UI      → http://localhost:9090                  │"
  echo "│  PostgreSQL    → localhost:5433  (fraud_admin/...)      │"
  echo "└─────────────────────────────────────────────────────────┘"
  echo ""
}

# ── Teardown ──────────────────────────────────────────────────
teardown() {
  echo "🛑  Stopping and removing all containers …"
  $COMPOSE down -v
  docker network rm docker_fraud 2>/dev/null || true
  echo "✅  Teardown complete."
}

# ── Main dispatcher ───────────────────────────────────────────
print_banner

COMMAND="${1:-help}"

case "$COMMAND" in
  up)
    setup_network
    start_services
    create_kafka_topics
    download_spark_jars
    deploy_spark_job
    show_urls
    ;;
  producer)
    run_producer
    ;;
  spark)
    run_spark
    ;;
  down)
    teardown
    ;;
  logs)
    SERVICE="${2:-airflow_webserver}"
    $COMPOSE logs -f "$SERVICE"
    ;;
  reset)
    teardown
    setup_network
    start_services
    create_kafka_topics
    deploy_spark_job
    show_urls
    ;;
  *)
    echo "Usage: ./run.sh [up|down|producer|spark|logs <service>|reset]"
    echo ""
    echo "  up        — Start all services, create topics, deploy Spark job"
    echo "  producer  — Run the transaction producer"
    echo "  spark     — Submit the Spark fraud detection job"
    echo "  down      — Stop and remove all containers + volumes"
    echo "  logs      — Tail logs (default: airflow_webserver)"
    echo "  reset     — Teardown + full restart"
    ;;
esac
