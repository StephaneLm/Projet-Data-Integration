import pandas as pd
import json
from kafka import KafkaProducer
import time

# Prodcer Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Charge les données en local depuis mon fichier raw_data
file_path = "../raw_data/FL_Dashboard_AY2009_2010_Q1.xls"
df = pd.read_excel(file_path)

# Envoies les données par paquets de 100 lignes
for i in range(0, len(df), 100):
    chunk = df.iloc[i:i+100]
    
    # Convertit chaque paquet du en JSON (chaque chunks)
    records = chunk.to_dict(orient='records') 
    for record in records:
        producer.send('student_loan_data', value=record)
    
    print(f"Sent batch {i // 100 + 1}")
    time.sleep(10)

print("Data streaming complété.")
