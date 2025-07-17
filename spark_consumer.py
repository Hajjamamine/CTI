from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
import logging
import sys

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('spark_consumer.log')  # Local file in current directory
    ]
)
logger = logging.getLogger(__name__)

# Also add a separate file handler for errors only
error_handler = logging.FileHandler('spark_consumer_errors.log')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(error_handler)

logger.info("Logging configured - writing to spark_consumer.log and spark_consumer_errors.log")

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
logger.info("Creating Spark session...")
spark = SparkSession.builder \
    .appName("CTI_Spark_Streaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

# Set Spark logging level
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session created successfully")
logger.info("MongoDB connector should be available now")

# Read from kafka
logger.info("Setting up Kafka consumer...")
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.0.230:9092") \
    .option("subscribe", "network-traffic") \
    .option("startingOffsets", "latest") \
    .load()

logger.info("Kafka consumer configured")

df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")
df_parsed = df_json.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
logger.info("JSON parsing configured")

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
logger.info("Cleaning column names...")
df_clean = clean_column_names(df_parsed)
logger.info(f"Column names cleaned. New columns: {df_clean.columns}")

# Load trained model (includes VectorAssembler + classifier)
logger.info("Loading trained model...")
model_path = "/home/jovyan/work/threat_model"
try:
    model = PipelineModel.load(model_path)
    logger.info("Model loaded successfully")
    logger.info(f"Model has {len(model.stages)} stages")
    for i, stage in enumerate(model.stages):
        logger.info(f"Stage {i}: {type(stage).__name__}")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise

# Optional: check for required features
logger.info("Checking for required features...")
# Find the VectorAssembler stage to get input columns
vector_assembler = None
for i, stage in enumerate(model.stages):
    logger.info(f"Examining stage {i}: {type(stage).__name__}")
    # Look specifically for VectorAssembler
    if type(stage).__name__ == 'VectorAssembler':
        try:
            vector_assembler = stage
            logger.info(f"Found VectorAssembler at stage {i}")
            logger.info(f"Required features: {len(stage.getInputCols())} columns")
            break
        except Exception as e:
            logger.warning(f"Error checking VectorAssembler stage {i}: {e}")
            continue

if vector_assembler:
    required_cols = vector_assembler.getInputCols()
    available_cols = df_clean.columns
    missing = [col for col in required_cols if col not in available_cols]
    if missing:
        logger.error(f"Missing expected columns: {missing}")
        logger.info(f"Available columns: {available_cols}")
        raise ValueError(f"Missing expected columns: {missing}")
    else:
        logger.info("All required features are available")
else:
    logger.warning("No VectorAssembler found in model pipeline - proceeding anyway")

# Predict directly (the model includes the assembler)
logger.info("Setting up model prediction...")
df_pred = model.transform(df_clean)
logger.info("Model transformation configured")

# Add simplified monitoring (reduced conflicts)
logger.info("Setting up simplified monitoring...")

# Only monitor the final predictions output
logger.info("Setting up prediction output monitoring...")
pred_debug = df_pred.select("prediction").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 3) \
    .trigger(processingTime='15 seconds') \
    .queryName("predictions_monitor") \
    .start()

logger.info("Prediction monitoring stream started")

# Write ALL predictions to local JSON files (avoiding MongoDB issues)
logger.info("6. Setting up JSON file writer (bypassing MongoDB for now)...")
try:
    logger.info("Creating JSON file write stream...")
    query = df_pred.writeStream \
        .format("json") \
        .option("path", "./predictions_output") \
        .option("checkpointLocation", "./checkpoint_json") \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .queryName("json_writer") \
        .start()
    
    logger.info("JSON file streaming started successfully")
    logger.info(f"Query ID: {query.id}")
    logger.info(f"Query Name: {query.name}")
    logger.info("Predictions will be saved to ./predictions_output/ directory")
    
    # Wait for a bit to see if data flows
    logger.info("Waiting 30 seconds to monitor data flow...")
    import time
    time.sleep(30)  # Wait 30 seconds to see debug output
    
    logger.info("Starting to await termination...")
    query.awaitTermination()
    
except Exception as e:
    logger.error(f"MongoDB streaming failed: {e}")
    logger.error(f"Exception type: {type(e).__name__}")
    import traceback
    logger.error(f"Full traceback: {traceback.format_exc()}")
    
    logger.info("Falling back to console output for ALL predictions...")
    
    # Alternative: Write all predictions to console for debugging
    try:
        query = df_pred.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime='5 seconds') \
            .queryName("console_fallback") \
            .start()
        
        logger.info("Console fallback stream started")
        query.awaitTermination()
        
    except Exception as fallback_e:
        logger.error(f"Even console fallback failed: {fallback_e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise