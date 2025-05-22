import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

while True:
    data = {
        "gudang_id": random.choice(gudangs),
        "suhu": random.randint(70, 90)
    }
    producer.send('sensor-suhu-gudang', value=data)
    print("Kirim suhu:", data)
    time.sleep(1)