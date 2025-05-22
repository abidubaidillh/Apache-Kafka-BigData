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