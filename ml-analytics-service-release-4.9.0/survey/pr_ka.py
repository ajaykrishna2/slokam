import json
from kafka import KafkaProducer
producer =KafkaProducer(bootstrap_servers='localhost:9092')
print(producer)

with open('daata.json') as f:
    data = json.load(f)
    print(data)
    # producer.s
    producer.send('s_data', data.encode('utf-8'))