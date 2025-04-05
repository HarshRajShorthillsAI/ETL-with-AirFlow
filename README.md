# 🛠️ Airflow ETL Pipeline with Docker, Celery, Redis, and PostgreSQL

This project demonstrates a fully functional **ETL pipeline using Apache Airflow**, containerized with **Docker Compose**, and backed by **CeleryExecutor**, **Redis** as broker, and **PostgreSQL** as the metadata database.

---

## 🚀 Features

- Apache Airflow with CeleryExecutor
- Redis as Celery broker
- PostgreSQL for metadata storage
- Dockerized environment for reproducibility
- Sample DAG (`etl_example`) to test pipeline execution

---

## 📁 Project Structure

├── dags/ # Contains Airflow DAGs
│ └── etl_example.py # Example ETL DAG
├── docker-compose.yml # Multi-service setup for Airflow
├── .env # Secrets and environment config
├── requirements.txt # (Optional) extra Python packages
└── README.md # Project documentation


---

## ⚙️ Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- At least 4GB of free memory

---

## 🛠️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/yourusername/airflow-etl-pipeline.git
cd airflow-etl-pipeline
```

### 2️⃣ Generate FERNET KEY
```python
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3️⃣ create .env file

```bash
# .env
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
AIRFLOW__CORE__EXECUTOR=CeleryExecutor

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
```

### 4️⃣ Initialize database
```bash
docker compose run --rm airflow-webserver airflow db init
```

### 5️⃣ Create Airflow user
```bash
docker compose run --rm airflow-webserver   airflow users create     --username admin\
--firstname Air\
--lastname Flow\
--role Admin\
--email example@gmail.com\
--password admin
```

### 6️⃣ Start All Services
```bash
docker-compose up -d
```

Services started:

- PostgreSQL

- Redis

- Airflow Webserver

- Airflow Scheduler

- Airflow Worker

### 7️⃣ Access the Airflow Web UI
Visit: http://localhost:8080

Login credentials:
```bash
Username:  admin
Password:  admin
```

## ✅ Verify the DAG
- On the Airflow UI, locate the DAG named etl_example

- Toggle it ON

- Click on "Trigger DAG" to run it manually

- Click the DAG name to view execution details and task instances

## Tear down
To stop and remove all containers:
```bash
docker-compose down
```

To also remove the volumes:
```bash
docker-compose down --volumes
```

## 🙋‍♂️ Questions or Contributions?
Feel free to open an issue or pull request if you'd like to contribute or need help getting started.