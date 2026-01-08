# Real-Time Fraud Detection Pipeline

[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-red)](https://redpanda.com/)
[![Flink](https://img.shields.io/badge/Flink-1.19-blue)](https://flink.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Production-grade streaming pipeline for real-time fraud detection, demonstrating Kafka + Flink + ML feature engineering with **ASEAN payment system context** (PromptPay, QRIS, GCash, PayNow).

## 🎯 Project Overview

This portfolio project showcases skills relevant to:
- Domain expertise in banking transactions, real-time risk, financial inclusion
- Deep streaming expertise with Kafka + Flink, stateful processing, ML integration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Transaction  │   Kafka    │     Flink      │  Feature  │   Risk    │
│  Generator   ──►  Topics  ──►  Processing  ──►  Store  ──►  Score   │
│  (Simulator)  │            │   (Stateful)   │  (Redis)  │  Output   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 17+ (for Flink job development)
- Python 3.11+ (for transaction generator)
- Maven 3.8+

### 1. Start Infrastructure

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/fraud-detection-pipeline.git
cd fraud-detection-pipeline

# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 2. Access Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Redpanda Console | http://localhost:8080 | - |
| Flink Dashboard | http://localhost:8081 | - |
| Redis Commander | http://localhost:8085 | - |
| Grafana | http://localhost:3000 | admin / fraud_admin |
| Prometheus | http://localhost:9090 | - |

### 3. Create Kafka Topics

```bash
# Create transactions topic
docker exec -it redpanda rpk topic create transactions \
  --partitions 4 \
  --replicas 1

# Create fraud-scores output topic
docker exec -it redpanda rpk topic create fraud-scores \
  --partitions 4 \
  --replicas 1

# Verify topics
docker exec -it redpanda rpk topic list
```

### 4. Run Transaction Generator

```bash
cd generator

# Install dependencies
pip install -r requirements.txt

# Run generator (100 events/sec, 2% fraud rate)
python transaction_generator.py \
  --bootstrap-servers localhost:19092 \
  --topic transactions \
  --rate 100 \
  --fraud-rate 0.02
```

### 5. Build & Deploy Flink Job

```bash
cd flink-jobs/fraud-processor

# Build the fat JAR
mvn clean package -DskipTests

# Submit to Flink cluster
docker cp target/fraud-processor-1.0.0-SNAPSHOT.jar flink-jobmanager:/opt/flink-jobs/
docker exec -it flink-jobmanager flink run /opt/flink-jobs/fraud-processor-1.0.0-SNAPSHOT.jar
```

## 📊 Architecture

### Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Event Source | Python Generator | Simulates ASEAN banking transactions |
| Message Broker | Redpanda (Kafka-compatible) | Event streaming with exactly-once semantics |
| Stream Processor | Apache Flink 1.19 | Stateful processing, windowed aggregations |
| Feature Store | Redis | Real-time feature serving for ML |
| Output Sink | Kafka + PostgreSQL | Scored transactions and audit trail |
| Monitoring | Prometheus + Grafana | Pipeline health and fraud metrics |

### Fraud Detection Rules

| Rule | Logic | Window |
|------|-------|--------|
| Velocity Check | >10 transactions in 1 hour | Tumbling 1hr |
| Amount Anomaly | Transaction > 3x 7-day average | Sliding 7 days |
| Geographic Impossible | 2 countries within 30 minutes | Sliding 30min |
| Rapid Small-Large | <$10 followed by >$500 in 5 min | CEP Pattern |
| New Device + High Value | First device txn AND amount >$200 | Stateful lookup |

### Real-Time Features

| Feature | Computation | Latency Target |
|---------|-------------|----------------|
| `txn_count_1h` | COUNT per user in sliding 1-hour | <50ms |
| `txn_amount_avg_7d` | AVG amount over 7 days | <100ms |
| `unique_merchants_24h` | COUNT DISTINCT merchants in 24h | <50ms |
| `time_since_last_txn` | Seconds since previous transaction | <20ms |
| `device_age_days` | Days since device first seen | <20ms |

## 🌏 ASEAN Payment Context

This project models Southeast Asian payment patterns:

| Payment Rail | Country | Characteristics |
|--------------|---------|-----------------|
| PromptPay | Thailand | Mobile proxy ID, instant P2P, high volume |
| QRIS | Indonesia | QR merchant payments, multi-wallet interop |
| GCash/PayMaya | Philippines | E-wallet, remittances, rural inclusion |
| PayNow | Singapore | NRIC/mobile proxy, cross-border |
| KHQR | Cambodia | QR payments, USD/KHR dual currency |

### Financial Inclusion Considerations
- **Thin-file users**: New digital users shouldn't be over-blocked
- **Remittance patterns**: Irregular but legitimate overseas worker transfers
- **Micro-merchants**: High volume, low value transactions
- **Cross-border corridors**: SG-TH, PH-Middle East patterns

## 📁 Project Structure

```
fraud-detection-pipeline/
├── docker-compose.yml          # Full stack orchestration
├── generator/                  # Transaction simulator (Python)
│   ├── transaction_generator.py
│   ├── requirements.txt
│   └── Dockerfile
├── flink-jobs/                 # Stream processing (Java)
│   └── fraud-processor/
│       ├── pom.xml
│       └── src/main/java/com/fraudpipeline/
│           ├── FraudDetectionJob.java
│           ├── events/         # POJOs
│           ├── features/       # Feature computation
│           └── rules/          # Fraud detection rules
├── schemas/                    # Avro schemas
│   ├── transaction.avsc
│   ├── fraud-score.avsc
│   └── postgres-init.sql
├── monitoring/                 # Prometheus + Grafana
│   ├── prometheus/
│   └── grafana/
└── docs/                       # Architecture diagrams
```

## 🎯 Success Metrics

| Metric | Target | Status |
|--------|--------|--------|
| End-to-end latency | <200ms p99 | 🔄 In Progress |
| Throughput | >1,000 events/sec | 🔄 In Progress |
| Feature freshness | <100ms to feature store | 🔄 In Progress |
| Fraud detection rate | >80% on test scenarios | 🔄 In Progress |

## 🛠 Development

### Running Tests

```bash
# Flink job tests
cd flink-jobs/fraud-processor
mvn test

# Generator tests (coming soon)
cd generator
pytest
```

### Local Development Without Docker

```bash
# Start Redpanda locally
rpk container start

# Start Redis
redis-server

# Run Flink locally
./bin/start-cluster.sh
```

## 📚 Learning Resources

- [Apache Flink Training](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/overview/)
- [Feast Feature Store](https://docs.feast.dev/)
- [Grab GrabDefence Case Study](https://engineering.grab.com/grab-defence)
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Chapter 11

## 🤝 Highlights

- How fraud detection patterns differ across ASEAN payment rails
- Balancing fraud prevention with financial inclusion goals
- Core banking integration patterns

- Exactly-once semantics with Flink checkpointing
- Stateful processing patterns and state backend choices
- Late event handling and watermark strategies
- Feature store architecture (online vs offline)

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

*Portfolio Project | Q2 2026 | Gunner*
