from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Définir le schéma JSON
schema = StructType([
    StructField("OPE ID", StringType(), True),
    StructField("School", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zip Code", StringType(), True),
    StructField("School Type", StringType(), True),
    StructField("Recipients", IntegerType(), True),
    StructField("# of Loans Originated", IntegerType(), True),
    StructField("$ of Loans Originated", DoubleType(), True),
    StructField("# of Disbursements", IntegerType(), True),
    StructField("$ of Disbursements", DoubleType(), True),
    StructField("uuid", StringType(), True),
])

# Lire le streaming Kafka dans Spark
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "student_loan_data") \
    .load()

# On parse les messages JSON là
json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
json_df.show()
