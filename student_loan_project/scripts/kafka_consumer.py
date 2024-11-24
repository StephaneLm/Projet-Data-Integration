from kafka import KafkaConsumer
import json

# Kafka Consumer
consumer = KafkaConsumer(
    'student_loan_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Afficher chaque message JSON bien formaté
for message in consumer:
    print(json.dumps(message.value, indent=4))  # Format lisible
