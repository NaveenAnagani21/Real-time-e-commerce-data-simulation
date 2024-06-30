from openpyxl import load_workbook
from confluent_kafka import Producer
import os, json, csv


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def excel_to_kafka(file_path, kafka_config, topic_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    parent_dir = os.path.dirname(current_dir)
    file_path = os.path.join(parent_dir, file_name)
    # for item in os.listdir(parent_dir):
    #         print('item',item)

    producer = Producer(kafka_config)
    # print("file_path=", file_path)

    with open(file_path, "r") as csvfile:
        csv_reader = csv.DictReader(csvfile)

        for row in csv_reader:
            producer.produce(
                topic_name,
                value=json.dumps(row).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

    producer.flush()


# Usage
file_name = "online_sales_data.csv"
kafka_config = {"bootstrap.servers": "localhost:9092", "client.id": "csv_producer"}
topic_name = "csv_data_topic"

excel_to_kafka(file_name, kafka_config, topic_name)
