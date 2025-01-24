from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

#initialising Spark session
spark = SparkSession.builder \
    .appName("Process Call History Data") \
    .getOrCreate()

#defining input and output bucket paths
INPUT_BUCKET = "gs://patient_call_history/*.csv"  
OUTPUT_BUCKET_TRANSFORMED = "gs://output_call_history/" 
OUTPUT_BUCKET_MISSING = "gs://missing-data-points/"

#defining schema for the call history data
schema = StructType([
    StructField("call_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("call_type", StringType(), True),
    StructField("resolution_status", StringType(), True),
    StructField("department", StringType(), True),
    StructField("call_duration", IntegerType(), True),
    StructField("notes", StringType(), True),
    StructField("follow_up_required", StringType(), True),
    StructField("satisfaction_score", IntegerType(), True),
    StructField("call_centre", StringType(), True)
])

#reading the input data from GCS
print("Reading data from GCP bucket...")
df = spark.read.csv(INPUT_BUCKET, schema=schema, header=True)

#identify rows with missing fields
print("Checking for missing fields...")
missing_data_df = df.filter(
    col("call_id").isNull() |
    col("patient_id").isNull() |
    col("timestamp").isNull() |
    col("call_type").isNull() |
    col("resolution_status").isNull() |
    col("department").isNull() |
    col("call_duration").isNull() |
    col("notes").isNull() |
    col("follow_up_required").isNull() |
    col("satisfaction_score").isNull() |
    col("call_centre").isNull()
)

#save missing data to a separate bucket
print(f"Found {missing_data_df.count()} rows with missing fields. Saving to {OUTPUT_BUCKET_MISSING}...")
missing_data_df.write.parquet(OUTPUT_BUCKET_MISSING, mode="append") 
print(f"Missing data saved to {OUTPUT_BUCKET_MISSING}")

#filter out rows with missing fields
print("Filtering out rows with missing fields...")
clean_data_df = df.na.drop()

#save clean data to a separate bucket
print(f"Writing clean data to GCP bucket: {OUTPUT_BUCKET_TRANSFORMED}...")
clean_data_df.write.parquet(OUTPUT_BUCKET_TRANSFORMED, mode="overwrite")
print(f"Clean data saved to {OUTPUT_BUCKET_TRANSFORMED}")

print("Process completed successfully.")
spark.stop()