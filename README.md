### Data-engineering-projects

### Wikimedia Kafka Pipeline

This project demonstrates a real-time data pipeline that ingests Wikimedia edit events, publishes them to **Apache Kafka**, and consumes them into **PostgreSQL** for storage and analysis.

The stack is containerized with **Docker Compose**.

##  Architecture

```
 Wikimedia EventStreams (SSE API)
              │
              ▼
         [ Producer ]
              │
              ▼
           [ Kafka ]
              │
              ▼
         [ Consumer ]
              │
              ▼
         [ Postgres ]
```

* **Producer** → Connects to Wikimedia EventStreams and pushes messages to Kafka.
* **Kafka** → Acts as the message broker.
* **Consumer** → Reads from Kafka and writes events into Postgres.
* **Postgres** → Stores structured data for querying.



##  Prerequisites

* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)



##  Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/ayushitomar03/wikimedia-kafka-pipeline.git
cd wikimedia-kafka-pipeline
```

### 2. Build images

```bash
docker-compose build
```

### 3. Start the pipeline

```bash
docker-compose up
```

### 4. Verify services

* Kafka UI: [http://localhost:8080](http://localhost:8080) *(if you added Kafdrop)*
* Postgres: `localhost:5432` (user: `user`, password: `password`, db: `wikimedia`)



##  Query Data

Connect to Postgres:

```bash
docker exec -it postgres psql -U user -d wikimedia
```

Example query:

```sql
SELECT COUNT(*) FROM wikimedia_events_2025_09;
SELECT * FROM wikimedia_events_2025_09 ORDER BY timestamp DESC LIMIT 10;
```
##  Challenges encountered during the pipeline 

Incorrect Kafka Ports

Using the correct Kafka ports is necessary for Docker containers to connect. Make sure to use the internal (`29092`) port for producer/consumer and expose the external (`9092`) port in Docker Compose.

Producer Stopped Unexpectedly

The producer sometimes stopped streaming data due to network errors or Wikimedia EventStreams errors (e.g., 403 Forbidden or ChunkedEncodingError).
Introduced retry loops, proper error handling, and SSE client headers to ensure the producer reconnects automatically and continues streaming.

Waiting for Kafka and Postgres

The consumer was attempting to read messages before Kafka was ready, leading to NoBrokersAvailable errors.
Added connection retries and waits to ensure Kafka is available before starting the consumer.
Similarly, the consumer waits for Postgres to be ready before inserting data.


## 📄 License

MIT License
