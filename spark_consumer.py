from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
import logging
import time
import os
from datetime import datetime

# Configure logging to file only
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Create timestamped log filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_directory, f"spark_consumer_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='w'
)
logger = logging.getLogger(__name__)

# Log the log file location
logger.info(f"Logging to file: {os.path.abspath(log_filename)}")

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
logger.info("Initializing Spark session with Kafka and MongoDB connectors...")
spark = SparkSession.builder \
    .appName("CTI_Spark_Streaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.mongodb.spark:mongo-spark-connector:10.5.0") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

logger.info("Spark session created successfully")

# Set Spark logging level
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark logging level set to WARN")

# Read from kafka
logger.info("Setting up Kafka stream connection...")
logger.info("Kafka broker: 192.168.1.166:9092")
logger.info("Kafka topic: network-traffic")
logger.info("Starting offset: latest")

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.33:9092") \
    .option("subscribe", "network-traffic") \
    .option("startingOffsets", "latest") \
    .load()

logger.info("Kafka stream configured successfully")
logger.info(f"Kafka DataFrame schema: {df_kafka.schema}")

df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")
logger.info("Kafka messages converted to JSON strings")

# Schema parsing
logger.info("Starting schema parsing...")
logger.info(f"Expected schema has {len(schema.fields)} fields")
df_parsed = df_json.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
logger.info("JSON messages parsed with predefined schema")
logger.info(f"Parsed DataFrame columns: {df_parsed.columns if hasattr(df_parsed, 'columns') else 'Schema not yet available'}")

# Clean column names
def clean_column_names(df):
    from collections import defaultdict

    renamed_cols = {}
    seen = defaultdict(int)

    for old_name in df.columns:
        # Clean the name
        new_name = old_name.strip().replace('/', '_').replace(' ', '_').replace('.', '_')

        # Ensure uniqueness
        if new_name in seen:
            seen[new_name] += 1
            new_name = f"{new_name}_{seen[new_name]}"
        else:
            seen[new_name] = 0

        renamed_cols[old_name] = new_name

    for old, new in renamed_cols.items():
        df = df.withColumnRenamed(old, new)

    return df



# Clean column names
logger.info("Starting column name cleaning...")
df_clean = clean_column_names(df_parsed)
logger.info(f"Column names cleaned. Total columns: {len(df_clean.columns) if hasattr(df_clean, 'columns') else 'Unknown'}")
logger.info(f"Cleaned column names: {df_clean.columns if hasattr(df_clean, 'columns') else 'Schema not yet available'}")

# Load trained model (includes VectorAssembler + classifier)
model_path = "/home/jovyan/work/threat_model"
logger.info(f"Loading trained model from: {model_path}")
try:
    model = PipelineModel.load(model_path)
    logger.info("Model loaded successfully")
    logger.info(f"Model has {len(model.stages)} stages")
    for i, stage in enumerate(model.stages):
        logger.info(f"Stage {i}: {type(stage).__name__}")
except Exception as e:
    logger.error(f"Failed to load model: {str(e)}")
    raise

# Optional: check for required features
# Find the VectorAssembler stage to get input columns
logger.info("Checking for VectorAssembler stage in model...")
vector_assembler = None
for i, stage in enumerate(model.stages):
    # Look specifically for VectorAssembler
    if type(stage).__name__ == 'VectorAssembler':
        try:
            vector_assembler = stage
            logger.info(f"Found VectorAssembler at stage {i}")
            break
        except Exception as e:
            logger.warning(f"Error accessing stage {i}: {str(e)}")
            continue

if vector_assembler:
    required_cols = vector_assembler.getInputCols()
    available_cols = df_clean.columns if hasattr(df_clean, 'columns') else []
    logger.info(f"Model requires {len(required_cols)} input columns")
    logger.info(f"Available columns: {len(available_cols)}")
    
    missing = [col for col in required_cols if col not in available_cols]
    if missing:
        logger.error(f"Missing expected columns: {missing}")
        raise ValueError(f"Missing expected columns: {missing}")
    else:
        logger.info("All required columns are available")
else:
    logger.warning("No VectorAssembler found in model stages")

# Predict directly (the model includes the assembler)
logger.info("Starting model transformation...")
df_pred = model.transform(df_clean)
logger.info("Model transformation completed")
logger.info(f"Prediction DataFrame schema: {df_pred.schema if hasattr(df_pred, 'schema') else 'Schema not available'}")

# Instead of writing the entire DataFrame (df), select only the required columns
filtered_df = df_pred.select([col for col in df_pred.columns if not any(vector_col in col for vector_col in ['raw_features', 'features', 'rawPrediction', 'probability'])])

# Add simplified monitoring (reduced conflicts)
logger.info("Setting up streaming queries...")

# Only monitor the final predictions output
logger.info("Starting prediction monitoring stream...")
pred_debug = filtered_df.select("prediction").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 3) \
    .trigger(processingTime='15 seconds') \
    .queryName("predictions_monitor") \
    .start()

logger.info("Prediction monitoring stream started successfully")
logger.info(f"Monitoring query ID: {pred_debug.id}")
logger.info("Trigger interval: 15 seconds")

# Write ALL predictions to MongoDB
logger.info("Setting up MongoDB connection...")
logger.info("MongoDB URI: mongodb://host.docker.internal:27017/")
logger.info("Database: cti_db")
logger.info("Collection: predictions")
logger.info("Checkpoint location: ./checkpoint_mongodb")

try:
    def write_to_mongodb(df, epoch_id):
        logger.info(f"MongoDB write batch - Epoch ID: {epoch_id}")
        logger.info(f"Batch size: {df.count()} records")
        
        try:
            df.write \
                .format("mongodb") \
                .mode("append") \
                .option("spark.mongodb.write.connection.uri", "mongodb://host.docker.internal:27017/") \
                .option("spark.mongodb.write.database", "cti_db") \
                .option("spark.mongodb.write.collection", "predictions") \
                .save()
            logger.info(f"Successfully wrote batch {epoch_id} to MongoDB")
        except Exception as e:
            logger.error(f"Failed to write batch {epoch_id} to MongoDB: {str(e)}")
            raise

    logger.info("Starting MongoDB streaming query...")
    query = filtered_df.writeStream \
        .foreachBatch(write_to_mongodb) \
        .option("checkpointLocation", "./checkpoint_mongodb") \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .queryName("mongodb_writer") \
        .start()
    
    logger.info("MongoDB streaming query started successfully")
    logger.info(f"MongoDB query ID: {query.id}")
    logger.info("MongoDB trigger interval: 5 seconds")
    
    # Wait for a bit to see if data flows
    logger.info("Waiting 30 seconds to observe data flow...")
    time.sleep(30)  # Wait 30 seconds to see debug output
    
    logger.info("Starting to await termination...")
    query.awaitTermination()
    
except Exception as e:
    logger.error(f"MongoDB writing failed: {str(e)}")
    import traceback
    logger.error(f"Full traceback: {traceback.format_exc()}")
    
    # Alternative: Write all predictions to console for debugging
    try:
        logger.info("Falling back to console output...")
        query = filtered_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .option("checkpointLocation", "./checkpoint_console") \
            .trigger(processingTime='5 seconds') \
            .queryName("console_fallback") \
            .start()
        
        logger.info("Console fallback stream started successfully")
        logger.info(f"Fallback query ID: {query.id}")
        query.awaitTermination()
        
    except Exception as fallback_e:
        logger.error(f"Console fallback also failed: {str(fallback_e)}")
        logger.error(f"Fallback traceback: {traceback.format_exc()}")
        raise fallback_e