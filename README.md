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
git clone https://github.com/<your-username>/wikimedia-kafka-pipeline.git
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


## 📄 License

MIT License
