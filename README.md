# Banking Transaction Streaming Pipeline

A data engineering mini-project that uses **Python, Apache Kafka, and Docker** to stream simulated banking transactions, validate transaction records, and store the processed data in a CSV file.

## Project Overview

This project demonstrates a basic real-time data streaming pipeline. A Python producer publishes transaction events to an Apache Kafka topic. A Python consumer reads the events, validates the transaction data, and saves the processed records to a CSV file for analysis.

## Architecture

```text
Sample Transactions (JSON)
          |
          v
   Python Producer
   (bank_producer.py)
          |
          | JSON Events
          v
    Apache Kafka
  Topic: bank-transactions
          |
          v
   Python Consumer
   (bank_consumer.py)
          |
          | Validation
          v
     CSV Output
(bank_transactions.csv)
```

## Technologies Used

- **Python** — transaction producer and consumer
- **Apache Kafka 3.9.1** — event streaming and message broker
- **Docker Desktop** — runs Kafka locally
- **kafka-python** — Python client for Kafka
- **JSON** — sample transaction input
- **CSV / Microsoft Excel** — processed output inspection

## Project Structure

```text
banking-transaction-streaming-pipeline/
├── bank_producer.py
├── bank_consumer.py
├── sample_transactions.json
├── requirements.txt
├── docker-compose.yml
├── bank_transactions.csv
└── README.md
```

## Features

- Publishes simulated banking transactions to Kafka.
- Supports deposit, withdrawal, and transfer transaction types.
- Adds a UTC event timestamp to each transaction.
- Validates required fields and transaction amounts.
- Checks whether the transaction type is supported.
- Writes processed records and their validation status to CSV.

## Prerequisites

- Python 3.10 or later
- Docker Desktop
- Git (optional, for version control)

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY.git
cd YOUR-REPOSITORY
```

Replace the URL with your actual GitHub repository URL.

### 2. Install Python dependencies

```bash
py -m pip install -r requirements.txt
```

### 3. Start Apache Kafka

Start Docker Desktop, then run:

```bash
docker compose up -d
```

### 4. Create the Kafka topic

```bash
docker exec order-stream-kafka /opt/kafka/bin/kafka-topics.sh --create --topic bank-transactions --bootstrap-server localhost:9092
```

If the topic already exists, you can verify it with:

```bash
docker exec order-stream-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Running the Pipeline

Open two PowerShell terminals in the project directory.

**Terminal 1 — Start the consumer**

```bash
py bank_consumer.py
```

**Terminal 2 — Run the producer**

```bash
py bank_producer.py
```

The producer publishes the sample transactions. The consumer reads the events, validates them, and writes the results to `bank_transactions.csv`.

The consumer is configured to stop after 60 seconds without receiving new messages.

## Output

The generated CSV contains the following fields:

| Field | Description |
|---|---|
| transaction_id | Unique transaction identifier |
| account_id | Simulated account identifier |
| transaction_type | Deposit, withdrawal, or transfer |
| amount | Transaction amount |
| event_time_utc | Event timestamp in UTC |
| processing_status | Validation result or error reason |

## Validation Rules

1. Required transaction fields must be present.
2. Transaction type must be `DEPOSIT`, `WITHDRAWAL`, or `TRANSFER`.
3. Transaction amount must be numeric.
4. Transaction amount must be greater than zero.

Valid records receive the status `VALID`. Invalid records receive a corresponding validation message.

## Results

The sample dataset contains five simulated transactions. On a successful first run with the appropriate Kafka topic and consumer-group offsets, the consumer should process these records and write their results to the CSV file.

Add screenshots of your actual execution, consumer output, and CSV results to document the implementation.

## Limitations

- Uses simulated transaction data rather than a real banking system.
- Does not perform actual money transfers or update account balances.
- Does not implement fraud detection.
- Uses CSV as a simple output destination rather than a production database.

## Future Enhancements

- Add unit tests and invalid transaction examples.
- Store processed records in PostgreSQL.
- Add a dead-letter Kafka topic for invalid events.
- Implement schema validation and structured logging.
- Add monitoring and a transaction analytics dashboard.

## Author

**Utkarsh Gaur**

## Disclaimer

This project is developed for educational purposes. All transaction records are simulated and do not represent real customers, accounts, or financial activity.
