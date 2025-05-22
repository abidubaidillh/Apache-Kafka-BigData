from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("GudangMonitoring") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema untuk suhu dan kelembaban
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# Baca stream suhu
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", expr("current_timestamp()"))

# Baca stream kelembaban
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", expr("current_timestamp()"))

# Filtering
peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)
peringatan_kelembaban = kelembaban_parsed.filter(col("kelembaban") > 70)

# Join stream
joined = suhu_parsed.join(
    kelembaban_parsed,
    expr("""
        suhu_parsed.gudang_id = kelembaban_parsed.gudang_id AND
        suhu_parsed.timestamp BETWEEN kelembaban_parsed.timestamp - interval 10 seconds AND kelembaban_parsed.timestamp + interval 10 seconds
    """),
    how="inner"
).select(
    suhu_parsed["gudang_id"],
    suhu_parsed["suhu"],
    kelembaban_parsed["kelembaban"]
).withColumn("status", expr("""
    CASE
        WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
        WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
        WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
        ELSE 'Aman'
    END
"""))

# Output stream
query = joined.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()