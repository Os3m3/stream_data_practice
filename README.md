# Real-Time Drilling Data Pipeline using Kafka, Schema Registry, and TimescaleDB

## Overview

This project simulates a real-time drilling environment inspired by
WITSML workflows used in the oil & gas industry.

The pipeline generates drilling measurements, streams them through
Apache Kafka, validates schema contracts using Schema Registry,
separates valid and invalid records, and stores trusted drilling data in
TimescaleDB for future analytics.


## What This Project Demonstrates

This project demonstrates practical data engineering skills including:

- Building real-time streaming pipelines
- Using Kafka topics for event-driven architecture
- Applying schema validation using Schema Registry
- Separating trusted and untrusted data
- Designing clean and bad data storage layers
- Working with time-series data using TimescaleDB
- Running infrastructure locally using Docker


### Key Features

-   Real-time drilling data simulation
-   Apache Kafka event streaming
-   Schema Registry integration
-   Data quality validation
-   Clean and bad data routing
-   Time-series storage using TimescaleDB
-   Dockerized infrastructure

------------------------------------------------------------------------

## Architecture

``` text
      FastAPI WITSML Simulator
            │
            ▼
      Kafka Producer
            │
            │ validates/serializes with Schema Registry
            ▼
      raw-drill-data Topic
            │
            ▼
      Validation Consumer
      │                  │
      ▼                  ▼
clean-drilling-data   bad-drilling-data
   │                  │
   ▼                  ▼
clean_drilling_data   bad_drilling_data
TimescaleDB           TimescaleDB
```

------------------------------------------------------------------------

## Data Flow

### Step 1 - Generate Drilling Data

A FastAPI service simulates drilling measurements such as:

-   Measured Depth (MD)
-   Rate of Penetration (ROP)
-   Weight on Bit (WOB)
-   RPM
-   Torque
-   Standpipe Pressure
-   Mud Flow
-   Hookload

### Step 2 - Stream Data to Kafka

The producer fetches drilling data and publishes events to:

``` text
raw-drill-data
```

### Step 3 - Schema Validation

Schema Registry ensures all events conform to the expected drilling
schema before entering the pipeline.

### Step 4 - Business Validation

The validation consumer checks:

-   Data length consistency
-   Negative values
-   Invalid drilling measurements
-   Out-of-range values

### Step 5 - Route Data

Valid records:

``` text
clean-drilling-data Topic
```

Invalid records:

``` text
bad-drilling-data Topic
```

### Step 6 - Store Data

Clean records are stored in:

``` text
clean_drilling_data
```

Invalid records are stored in:

``` text
bad_drilling_data
```

Both tables are managed using TimescaleDB.

------------------------------------------------------------------------

## Technology Stack

| Component | Technology |
|-----------|------------|
| API Simulation | FastAPI |
| Streaming | Apache Kafka |
| Schema Management | Schema Registry |
| Data Processing | Python |
| Storage | TimescaleDB |
| Containerization | Docker |

------------------------------------------------------------------------

## Validation Rules

Current validation includes:

- `mnemonic_list` length must match `data` length
- Measurements must not contain invalid negative values
- RPM must be within the expected demo range
- Pressure and flow values must be valid
- Invalid records are separated from clean records

## Database Design

### `clean_drilling_data Table`

Stores trusted clean drilling measurements.

Example columns:

```text
time
sequence_number
well_uid
wellbore_uid
rig_id
measured_depth_m
rop_m_per_hr
wob_klbf
rpm
torque_ftlb
standpipe_pressure_psi
mud_flow_lpm
hookload_klbf
```

### `bad_drilling_data Table`

Stores invalid records for investigation.

Example columns:

```text
time
sequence_number
well_uid
wellbore_uid
rig_id
measured_depth_m
rop_m_per_hr
wob_klbf
rpm
torque_ftlb
standpipe_pressure_psi
mud_flow_lpm
hookload_klbf
```

## How to Run

### 1. Environment Variables

Create a `.env` file in the project root:

```env
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
```

### 2. Start Docker services

```bash
docker compose up -d
```

This starts the required infrastructure such as Kafka, Schema Registry, Kafka UI, and TimescaleDB.

Update the values based on your Docker Compose database configuration.


## Option A: Run with uv

### 3. Install dependencies with uv

```bash
uv sync
```

### 4. Start the WITSML simulator API

```bash
uv run uvicorn api:app --reload
```

### 5. Start the Kafka producer

Open a new terminal:

```bash
uv run python producer.py
```

### 6. Start the validation consumer

Open a new terminal:

```bash
uv run python validation_consumer.py
```

### 7. Start the database consumer for clean data storing

Open a new terminal:

```bash
uv run python clean_data_to_db_consumer.py
```

### 8. Start the database consumer for bad data storing

Open a new terminal:

```bash
uv run python bad_data_to_db_consumer.py
```


---

## Option B: Run without uv using Python venv and pip

### 3. Create and activate a virtual environment

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 4. Install dependencies with pip

```bash
pip install -r requirements.txt
```

### 5. Start the WITSML simulator API

```bash
uvicorn api:app --reload
```

### 6. Start the Kafka producer

Open a new terminal and activate the virtual environment again:

```bash
python producer.py
```

### 7. Start the validation consumer

Open a new terminal and activate the virtual environment again:

```bash
python validation_consumer.py
```

### 8. Start the database consumer for clean data storing

Open a new terminal and activate the virtual environment again:

```bash
python clean_data_to_db_consumer.py
```

### 9. Start the database consumer for bad data storing

Open a new terminal and activate the virtual environment again:

```bash
python bad_data_to_db_consumer.py
```

---


## Verify the Pipeline

Check the following:

```text
Kafka UI:
- raw-drill-data
- clean-drilling-data
- bad-drilling-data

TimescaleDB:
- clean_drilling_data
- bad_drilling_data

Application logs:
- Producer delivery logs
- Validation routing logs
- Database insert logs
```

## Project Structure

```text
project/
├── api.py
├── producer.py
├── validation_consumer.py
├── requirements.txt
├── clean_data_to_db_consumer.py
├── bad_data_to_db_consumer.py
├── schema.py
├── logger.py
├── docker-compose.yml
├── .env
└── README.md
```

## Future Improvements

- Add Grafana real-time monitoring
- Add OpenMetadata integration
- Add automated data quality reports
- Add alerting for drilling anomalies
- Add authentication and authorization
- Add cloud deployment
- Add CI/CD pipeline
- Add monitoring for Kafka consumer lag

## Purpose

This project was built to demonstrate how real-time drilling data can be streamed, validated, separated, and stored using modern data engineering tools commonly used in industrial data platforms.