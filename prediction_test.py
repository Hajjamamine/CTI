from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# 1. Start Spark session
spark = SparkSession.builder \
    .appName("TestModel") \
    .master("local[*]") \
    .getOrCreate()

# Optional: suppress warnings
spark.sparkContext.setLogLevel("WARN")

# 2. Load test dataset
test_df = spark.read.csv(r"C:\Users\PC\Desktop\CTI\test_dataset.csv", header=True, inferSchema=True)

# 3. Clean column names
test_df = test_df.toDF(*[col.strip().replace(" ", "_").replace(".", "_").replace("/", "_") for col in test_df.columns])

# 4. Extract true labels (for evaluation) and drop 'Label' for prediction
true_labels = test_df.select("Label")
test_df_without_labels = test_df.drop("Label")

# 5. Load the saved ML pipeline model (no file:/// needed)
model_path = r"C:\Users\PC\Desktop\CTI\threat_detection_model_RF"
pipeline_model = PipelineModel.load(model_path)

# 6. Run predictions
predictions = pipeline_model.transform(test_df_without_labels)

# 7. Show prediction results
predictions.select("prediction").show(5)
