from confluent_kafka import Consumer, KafkaError
import json

def csv_consumer(kafka_config, topic_name, group_id):
    # Update the kafka_config with the group_id
    kafka_config['group.id'] = group_id
    kafka_config['auto.offset.reset'] = 'earliest'

    consumer = Consumer(kafka_config)
    consumer.subscribe([topic_name])

    try:
        print(f"Consumer {group_id} started. Listening for messages...")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print(f'Error: {msg.error()}')
            else:
                # Decode and parse the message
                try:
                    record = json.loads(msg.value().decode('utf-8'))
                    print(f"Consumer {group_id} received: {record}")
                except json.JSONDecodeError:
                    print(f"Error decoding message: {msg.value()}")

    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        consumer.close()

# Usage
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'csv_consumer'
}
topic_name = 'csv_data_topic'
group_id = 'csv_consumer_group'

csv_consumer(kafka_config, topic_name, group_id)