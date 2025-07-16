from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler

schema = StructType([
    StructField(" Destination Port", DoubleType()),
    StructField(" Flow Duration", DoubleType()),
    StructField(" Total Fwd Packets", DoubleType()),
    StructField(" Total Backward Packets", DoubleType()),
    StructField("Total Length of Fwd Packets", DoubleType()),
    StructField(" Total Length of Bwd Packets", DoubleType()),
    StructField(" Fwd Packet Length Max", DoubleType()),
    StructField(" Fwd Packet Length Min", DoubleType()),
    StructField(" Fwd Packet Length Mean", DoubleType()),
    StructField(" Fwd Packet Length Std", DoubleType()),
    StructField("Bwd Packet Length Max", DoubleType()),
    StructField(" Bwd Packet Length Min", DoubleType()),
    StructField(" Bwd Packet Length Mean", DoubleType()),
    StructField(" Bwd Packet Length Std", DoubleType()),
    StructField("Flow Bytes/s", DoubleType()),
    StructField(" Flow Packets/s", DoubleType()),
    StructField(" Flow IAT Mean", DoubleType()),
    StructField(" Flow IAT Std", DoubleType()),
    StructField(" Flow IAT Max", DoubleType()),
    StructField(" Flow IAT Min", DoubleType()),
    StructField("Fwd IAT Total", DoubleType()),
    StructField(" Fwd IAT Mean", DoubleType()),
    StructField(" Fwd IAT Std", DoubleType()),
    StructField(" Fwd IAT Max", DoubleType()),
    StructField(" Fwd IAT Min", DoubleType()),
    StructField("Bwd IAT Total", DoubleType()),
    StructField(" Bwd IAT Mean", DoubleType()),
    StructField(" Bwd IAT Std", DoubleType()),
    StructField(" Bwd IAT Max", DoubleType()),
    StructField(" Bwd IAT Min", DoubleType()),
    StructField("Fwd PSH Flags", DoubleType()),
    StructField(" Bwd PSH Flags", DoubleType()),
    StructField(" Fwd URG Flags", DoubleType()),
    StructField(" Bwd URG Flags", DoubleType()),
    StructField(" Fwd Header Length", DoubleType()),
    StructField(" Bwd Header Length", DoubleType()),
    StructField("Fwd Packets/s", DoubleType()),
    StructField(" Bwd Packets/s", DoubleType()),
    StructField(" Min Packet Length", DoubleType()),
    StructField(" Max Packet Length", DoubleType()),
    StructField(" Packet Length Mean", DoubleType()),
    StructField(" Packet Length Std", DoubleType()),
    StructField(" Packet Length Variance", DoubleType()),
    StructField("FIN Flag Count", DoubleType()),
    StructField(" SYN Flag Count", DoubleType()),
    StructField(" RST Flag Count", DoubleType()),
    StructField(" PSH Flag Count", DoubleType()),
    StructField(" ACK Flag Count", DoubleType()),
    StructField(" URG Flag Count", DoubleType()),
    StructField(" CWE Flag Count", DoubleType()),
    StructField(" ECE Flag Count", DoubleType()),
    StructField(" Down/Up Ratio", DoubleType()),
    StructField(" Average Packet Size", DoubleType()),
    StructField(" Avg Fwd Segment Size", DoubleType()),
    StructField(" Avg Bwd Segment Size", DoubleType()),
    StructField(" Fwd Header Length.1", DoubleType()),
    StructField("Fwd Avg Bytes/Bulk", DoubleType()),
    StructField(" Fwd Avg Packets/Bulk", DoubleType()),
    StructField(" Fwd Avg Bulk Rate", DoubleType()),
    StructField(" Bwd Avg Bytes/Bulk", DoubleType()),
    StructField(" Bwd Avg Packets/Bulk", DoubleType()),
    StructField("Bwd Avg Bulk Rate", DoubleType()),
    StructField("Subflow Fwd Packets", DoubleType()),
    StructField(" Subflow Fwd Bytes", DoubleType()),
    StructField(" Subflow Bwd Packets", DoubleType()),
    StructField(" Subflow Bwd Bytes", DoubleType()),
    StructField("Init_Win_bytes_forward", DoubleType()),
    StructField(" Init_Win_bytes_backward", DoubleType()),
    StructField(" act_data_pkt_fwd", DoubleType()),
    StructField(" min_seg_size_forward", DoubleType()),
    StructField("Active Mean", DoubleType()),
    StructField(" Active Std", DoubleType()),
    StructField(" Active Max", DoubleType()),
    StructField(" Active Min", DoubleType()),
    StructField("Idle Mean", DoubleType()),
    StructField(" Idle Std", DoubleType()),
    StructField(" Idle Max", DoubleType()),
    StructField(" Idle Min", DoubleType())
])

# Spark session with Kafka & MongoDB connectors
spark = SparkSession.builder \
    .appName("CTI_Spark_Streaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.mongodb.spark:mongodb-spark-connector_2.12:10.1.1") \
    .getOrCreate()



# Read from kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.0.230:9092") \
    .option("subscribe", "network-traffic") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")
df_parsed = df_json.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Clean column names
def clean_column_names(df):
    for old in df.columns:
        new = old.lstrip().replace('/', '_').replace(' ', '_')
        df = df.withColumnRenamed(old, new)
    return df

df_clean = clean_column_names(df_parsed)

# Assemble features
feature_cols = df_clean.columns
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_vector = assembler.transform(df_clean)

# Load trained mpdel
model_path = "/home/jovyan/work/threat_model"  
model = PipelineModel.load(model_path)

# Predict
df_pred = model.transform(df_vector)

# Filter predictions for anomalies
df_alerts = df_pred.filter((col("prediction") == 1) | (col("prediction") == 2))

# Write alerts to MongoDB
query = df_alerts.writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.database", "cti_db") \
    .option("spark.mongodb.collection", "alerts") \
    .option("checkpointLocation", "/tmp/checkpoint_cti") \
    .outputMode("append") \
    .start()


query.awaitTermination()