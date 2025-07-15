import os
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# --- Set environment for consistent Python versions ---
os.environ["PYSPARK_PYTHON"] = "/opt/conda/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/conda/bin/python3"

# --- Start Spark session ---
spark = SparkSession.builder \
    .appName("ThreatModelPrediction") \
    .master("local[*]") \
    .getOrCreate()

# --- Load and clean test dataset ---
test_df = spark.read.csv("test_dataset.csv", header=True, inferSchema=True)
test_df = test_df.toDF(*[col.strip().replace(" ", "_").replace(".", "_").replace("/", "_") for col in test_df.columns])

# --- Extract labels and features ---
true_labels = test_df.select("Label")
test_df_without_labels = test_df.drop("Label")

# --- Load trained pipeline model ---
model_path = "threat_model"
pipeline_model = PipelineModel.load(model_path)

# --- Run prediction ---
predictions = pipeline_model.transform(test_df_without_labels)

# --- Reattach true labels using row index ---
predictions_with_id = predictions.withColumn("row_id", monotonically_increasing_id())
true_labels_with_id = true_labels.withColumn("row_id", monotonically_increasing_id())
predictions_with_labels = predictions_with_id.join(true_labels_with_id, on="row_id").drop("row_id")

# --- Show predictions with true labels ---
predictions_with_labels.select("prediction", "Label").show(10)


# --- Stop Spark session ---
spark.stop()
