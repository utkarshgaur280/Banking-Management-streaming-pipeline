
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "bank-transactions"

transactions = [
    {
        "transaction_id": "TXN1001",
        "account_id": "ACC101",
        "transaction_type": "DEPOSIT",
        "amount": 5000.00
    },
    {
        "transaction_id": "TXN1002",
        "account_id": "ACC102",
        "transaction_type": "WITHDRAWAL",
        "amount": 1500.00
    },
    {
        "transaction_id": "TXN1003",
        "account_id": "ACC101",
        "transaction_type": "TRANSFER",
        "amount": 2000.00
    },
    {
        "transaction_id": "TXN1004",
        "account_id": "ACC103",
        "transaction_type": "DEPOSIT",
        "amount": 12500.00
    },
    {
        "transaction_id": "TXN1005",
        "account_id": "ACC102",
        "transaction_type": "WITHDRAWAL",
        "amount": 700.00
    }
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    try:
        for transaction in transactions:
            transaction["event_time_utc"] = datetime.now(
                timezone.utc
            ).isoformat()

            producer.send(TOPIC, value=transaction).get(timeout=10)

            print("Published:", json.dumps(transaction))
            time.sleep(0.5)

        producer.flush()
        print("\nAll banking transactions published successfully.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
