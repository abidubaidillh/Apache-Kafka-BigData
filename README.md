# Tugas Apache-Kafka BIGDATA
## Big Data B

|Nama|NRP|
|-|-|
|Abid Ubaidillah A|5027231089|
***

# Apache-Kafka

🎯 Latar Belakang Masalah
Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:

- Sensor Suhu

- Sensor Kelembaban

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.

📋 Tugas Mahasiswa
1. Buat Topik Kafka
Buat dua topik di Apache Kafka:

- sensor-suhu-gudang

- sensor-kelembaban-gudang

Topik ini akan digunakan untuk menerima data dari masing-masing sensor secara real-time.

2. Simulasikan Data Sensor (Producer Kafka)
Buat dua Kafka producer terpisah:

  a. Producer Suhu
Kirim data setiap detik

  Format:

{"gudang_id": "G1", "suhu": 82}
  b. Producer Kelembaban
Kirim data setiap detik

  Format:

{"gudang_id": "G1", "kelembaban": 75}
Gunakan minimal 3 gudang: G1, G2, G3.

3. Konsumsi dan Olah Data dengan PySpark
  a. Buat PySpark Consumer
Konsumsi data dari kedua topik Kafka.

  b. Lakukan Filtering:
Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi

Kelembaban > 70% → tampilkan sebagai peringatan kelembaban tinggi

Contoh Output:
[Peringatan Suhu Tinggi] Gudang G2: Suhu 85°C [Peringatan Kelembaban Tinggi] Gudang G3: Kelembaban 74%
4. Gabungkan Stream dari Dua Sensor
Lakukan join antar dua stream berdasarkan gudang_id dan window waktu (misalnya 10 detik) untuk mendeteksi kondisi bahaya ganda.

  c. Buat Peringatan Gabungan:
Jika ditemukan suhu > 80°C dan kelembaban > 70% pada gudang yang sama, tampilkan peringatan kritis.

✅ Contoh Output Gabungan:
[PERINGATAN KRITIS] Gudang G1: - Suhu: 84°C - Kelembaban: 73% - Status: Bahaya tinggi! Barang berisiko rusak Gudang G2: - Suhu: 78°C - Kelembaban: 68% - Status: Aman Gudang G3: - Suhu: 85°C - Kelembaban: 65% - Status: Suhu tinggi, kelembaban normal Gudang G4: - Suhu: 79°C - Kelembaban: 75% - Status: Kelembaban tinggi, suhu aman

### Langkah Pengerjaannya

- Membuat Struktur direktori 
```
project/
│
├── docker-compose.yml
├── kafka/
│   └── ...
├── producers/
│   ├── suhu_producer.py
│   └── kelembaban_producer.py
├── spark_app/
    └── consumer_spark.py


```

- Isi File `docker-compose.yml`, `suhu_producer.py`, `kelembaban_producer.py` dan `consumer_stream.py`

### docker-compose.yml
```
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.2.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.2.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper

  spark:
    image: bitnami/spark:3
    depends_on:
      - kafka
    environment:
      - SPARK_MODE=master

```

- Cara Menjalankan docker `docker compose up -d`
- Cek container yang telah dibuat `docker ps`

![image](https://github.com/user-attachments/assets/33f8f05a-d811-484e-9ea7-37489a6d7e6b)

### suhu_producer.py
```
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
```

#### Install kafka-python
- python3 -m venv venv
- source venv/bin/active
- pip install kafka-python
 
#### Cara Menjalankannya di folder big data `python3 producer/suhu_producer.py`

![Screenshot 2025-05-22 233556](https://github.com/user-attachments/assets/4f0be989-c329-482c-8a9e-9d7941abebd1)

### kelembaban_producer.py
```
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
        "kelembaban": random.randint(60, 80)
    }
    producer.send('sensor-kelembaban-gudang', value=data)
    print("Kirim kelembaban:", data)
    time.sleep(1)
```

#### Install kafka-python
- python3 -m venv venv
- source venv/bin/active
- pip install kafka-python
 
#### Cara Menjalankannya di folder big data `python3 producer/kelembaban_producer.py`

![Screenshot 2025-05-22 233614](https://github.com/user-attachments/assets/472dcade-c70c-4f06-aa89-80043481c39e)


### consumer_stream.py
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Membuat Spark session
spark = SparkSession.builder \
    .appName("SensorGudangConsumer") \
    .getOrCreate()

# Untuk mengurangi log yang terlalu banyak
spark.sparkContext.setLogLevel("WARN")

# Skema untuk suhu
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

# Skema untuk kelembaban
schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Baca stream suhu
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Baca stream kelembaban
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse JSON
suhu_data = suhu_df.select(from_json(col("value"), schema_suhu).alias("data")).select("data.*")
kelembaban_data = kelembaban_df.select(from_json(col("value"), schema_kelembaban).alias("data")).select("data.*")

# Filter suhu > 80
suhu_alert = suhu_data.filter(col("suhu") > 80)

# Filter kelembaban > 70
kelembaban_alert = kelembaban_data.filter(col("kelembaban") > 70)

# Tampilkan ke console suhu tinggi
suhu_query = suhu_alert.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Tampilkan ke console kelembaban tinggi
kelembaban_query = kelembaban_alert.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()

```
