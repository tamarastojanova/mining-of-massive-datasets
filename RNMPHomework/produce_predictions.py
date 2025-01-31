import os
import numpy
import pandas as pd
from joblib import load
from xgboost import XGBClassifier
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StructType, StructField, FloatType

os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-8.0.442.6-hotspot"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Python312\\python.exe"
os.environ["PYSPARK_PYTHON"] = "C:\\Python312\\python.exe"
os.environ[
    "PYSPARK_SUBMIT_ARGS"] = '--driver-class-path "C:\\Program Files\\Eclipse Adoptium\\jdk-8.0.442.6-hotspot\\bin" pyspark-shell'

spark = SparkSession.builder \
    .appName("DiabetesIndicators") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config("spark.driver.host", "192.168.100.11") \
    .getOrCreate()

model = load("best_model_XGB.joblib")
scaler = load("scaler.joblib")

schema = StructType([
    StructField("HighBP", DoubleType()),
    StructField("HighChol", DoubleType()),
    StructField("CholCheck", DoubleType()),
    StructField("BMI", DoubleType()),
    StructField("Smoker", DoubleType()),
    StructField("Stroke", DoubleType()),
    StructField("HeartDiseaseorAttack", DoubleType()),
    StructField("PhysActivity", DoubleType()),
    StructField("Fruits", DoubleType()),
    StructField("Veggies", DoubleType()),
    StructField("HvyAlcoholConsump", DoubleType()),
    StructField("AnyHealthcare", DoubleType()),
    StructField("NoDocbcCost", DoubleType()),
    StructField("GenHlth", DoubleType()),
    StructField("MentHlth", DoubleType()),
    StructField("PhysHlth", DoubleType()),
    StructField("DiffWalk", DoubleType()),
    StructField("Sex", DoubleType()),
    StructField("Age", DoubleType()),
    StructField("Education", DoubleType()),
    StructField("Income", DoubleType())
])

data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health_data") \
    .option("startingOffsets", "latest") \
    .load()

parsed_data = data.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*")


@pandas_udf(FloatType())
def predict_udf(*cols):
    input_data = pd.concat(cols, axis=1)
    input_data.columns = [
        "HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
        "HeartDiseaseorAttack", "PhysActivity", "Fruits", "Veggies",
        "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth",
        "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income"
    ]
    scaled_data = scaler.transform(input_data)
    predictions = model.predict(scaled_data)
    return pd.Series(predictions)


predictions_df = parsed_data.withColumn("predicted_class", predict_udf(*parsed_data.columns))

output_df = predictions_df.select(
    F.to_json(F.struct("*")).alias("value")
)

query = output_df \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "health_data_predicted") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
