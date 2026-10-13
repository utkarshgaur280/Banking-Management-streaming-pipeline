
import csv
import json
import os
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC = "bank-transactions"
OUTPUT_FILE = "bank_transactions.csv"

FIELDS = [
    "transaction_id",
    "account_id",
    "transaction_type",
    "amount",
    "event_time_utc",
    "processing_status"
]

VALID_TYPES = {"DEPOSIT", "WITHDRAWAL", "TRANSFER"}


def validate_transaction(transaction):
    required = [
        "transaction_id",
        "account_id",
        "transaction_type",
        "amount",
        "event_time_utc"
    ]

    if not all(field in transaction for field in required):
        return False, "Missing required field"

    if transaction["transaction_type"] not in VALID_TYPES:
        return False, "Invalid transaction type"

    try:
        amount = float(transaction["amount"])
    except (ValueError, TypeError):
        return False, "Amount must be numeric"

    if amount <= 0:
        return False, "Amount must be greater than zero"

    return True, "VALID"


def main():
    file_exists = (
        os.path.exists(OUTPUT_FILE)
        and os.path.getsize(OUTPUT_FILE) > 0
    )

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="banking-transaction-processor",
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        ),
        consumer_timeout_ms=60000
    )

    processed = 0
    valid_count = 0
    invalid_count = 0

    print("Listening for banking transactions...")
    print("The consumer will stop after 60 seconds without new messages.")

    try:
        with open(
            OUTPUT_FILE, "a", newline="", encoding="utf-8"
        ) as output:
            writer = csv.DictWriter(output, fieldnames=FIELDS)

            if not file_exists:
                writer.writeheader()

            for message in consumer:
                transaction = message.value
                is_valid, reason = validate_transaction(transaction)

                record = {
                    "transaction_id": transaction.get("transaction_id", ""),
                    "account_id": transaction.get("account_id", ""),
                    "transaction_type": transaction.get("transaction_type", ""),
                    "amount": transaction.get("amount", ""),
                    "event_time_utc": transaction.get("event_time_utc", ""),
                    "processing_status": reason
                }

                writer.writerow(record)
                output.flush()

                processed += 1

                if is_valid:
                    valid_count += 1
                    print(
                        f"Processed {record['transaction_id']}: "
                        f"{record['transaction_type']} "
                        f"Rs.{record['amount']} - VALID"
                    )
                else:
                    invalid_count += 1
                    print(
                        f"Rejected {record['transaction_id']}: {reason}"
                    )

                consumer.commit()

    finally:
        consumer.close()

    print("\nProcessing completed.")
    print("Total transactions:", processed)
    print("Valid transactions:", valid_count)
    print("Invalid transactions:", invalid_count)
    print("Output file:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
