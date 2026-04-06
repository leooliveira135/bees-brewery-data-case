# 🍺 BEES Brewery Data Engineering Pipeline

## 🚀 Overview

This project implements a **production-style data pipeline** that
ingests brewery data from the Open Brewery API and processes it using a
**Medallion Architecture (Bronze → Silver → Gold)**.

It demonstrates real-world data engineering practices including
orchestration, cloud storage, transformation, and analytical querying.

------------------------------------------------------------------------

## 🏗️ Architecture

``` text
        Open Brewery API
                │
                ▼
        [Extract - Python]
                │
                ▼
        🟤 Bronze Layer (Raw - S3)
                │
                ▼
        ⚪ Silver Layer (Cleaned - Spark)
                │
                ▼
        🟡 Gold Layer (Aggregated - Spark)
                │
                ▼
        AWS Glue Catalog
                │
                ▼
            Athena (SQL)
```

------------------------------------------------------------------------

## ⚙️ Tech Stack

-   **Languages:** Python, SQL\
-   **Processing:** PySpark + Delta Lake\
-   **Orchestration:** Apache Airflow\
-   **Cloud:** AWS (S3, Glue, Athena)\
-   **Infrastructure:** Terraform\
-   **Containers:** Docker & Docker Compose

------------------------------------------------------------------------

## 📂 Project Structure

    dags/
      └── bees/openbrewery/
          ├── dags/                # Airflow DAGs
          ├── src/etl/             # ETL logic
          ├── src/aws/             # AWS integrations
          ├── src/queries/         # Athena queries
          └── src/setup/           # Configurations

------------------------------------------------------------------------

## 🔄 Pipeline Flow

1.  **Extract**
    -   Pulls brewery data from Open Brewery API
    -   Stores raw JSON into S3 (Bronze)
2.  **Transform**
    -   Cleans schema inconsistencies
    -   Handles nulls and duplicates
    -   Writes structured data (Silver)
3.  **Aggregate**
    -   Creates analytics-ready datasets
    -   Example: breweries per state/type (Gold)
4.  **Catalog & Query**
    -   Registers tables in AWS Glue
    -   Query using Athena

------------------------------------------------------------------------

## ▶️ Running Locally

### Prerequisites

-   Docker
-   Docker Compose
-   AWS credentials configured

### Start Environment

``` bash
docker-compose up --build
```

### Access Airflow

-   URL: http://localhost:8080
-   Trigger DAG: `openbrewery`

------------------------------------------------------------------------

## 📊 Example Use Cases

-   Count breweries by location
-   Analyze brewery types distribution
-   Enable BI dashboards on Athena

------------------------------------------------------------------------

## 💡 Key Highlights

-   End-to-end pipeline (ingestion → analytics)
-   Cloud-native architecture
-   Reproducible environment
-   Modular and scalable design
-   Real-world orchestration with Airflow

------------------------------------------------------------------------

## 🔧 Suggested Improvements

To take this project to production-grade:

-   Add **data quality validation (Great Expectations)**
-   Implement **CI/CD (GitHub Actions)**
-   Add **monitoring (Prometheus/Grafana)**
-   Include **unit + integration tests**
-   Add **partitioning strategy in S3**
-   Optimize Spark jobs (performance tuning)

------------------------------------------------------------------------

## 📌 Repository

https://github.com/leooliveira135/bees-brewery-data-case

------------------------------------------------------------------------

## 🧠 Why This Project Matters

This project demonstrates: - Understanding of modern data
architectures - Ability to work with cloud-native tools - Experience
with orchestration and distributed processing

It's not just a demo --- it's a **foundation for real data platforms**.
