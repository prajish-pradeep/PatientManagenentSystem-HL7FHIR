from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, expr

#initialising Spark session
spark = SparkSession.builder \
    .appName("Feedback History Data Processing") \
    .getOrCreate()

#defining input and output bucket paths
INPUT_BUCKET = "gs://feedback_history/*.csv" 
OUTPUT_BUCKET_CLEAN = "gs://output_feedback_history/clean_feedback_history" 
OUTPUT_BUCKET_MISSING = "gs://missing-data-points/feedback_history_missing_data" 

#reading input data from GCS bucket
print("Reading data from GCP bucket...")
data = spark.read.csv(INPUT_BUCKET, header=True, inferSchema=True)

#checking for missing fields
print("Checking for missing fields...")
missing_data = data.filter(expr(" OR ".join([f"{col} IS NULL" for col in data.columns])))
non_missing_data = data.dropna()

#saving missing data to GCP bucket
print(f"Found {missing_data.count()} rows with missing fields. Saving to {OUTPUT_BUCKET_MISSING}...")
missing_data.write \
    .mode("append") \
    .parquet(OUTPUT_BUCKET_MISSING)
print(f"Missing data saved to {OUTPUT_BUCKET_MISSING}")

#list of numeric columns to apply rounding
numeric_columns = [
    "overall_score",
    "satisfaction_score",
    "professionalism_score",
    "response_time_score",
    "resolution_score"
]

#round numeric columns to the nearest integer
print("Rounding numeric scores to the nearest integer...")
for column in numeric_columns:
    non_missing_data = non_missing_data.withColumn(column, round(col(column)))

#saving clean data to gcp bucket
print(f"Writing clean data to GCP bucket: {OUTPUT_BUCKET_CLEAN}...")
non_missing_data.write \
    .mode("overwrite") \
    .parquet(OUTPUT_BUCKET_CLEAN)
print(f"Clean data saved to {OUTPUT_BUCKET_CLEAN}")

print("Process completed successfully.")
spark.stop()