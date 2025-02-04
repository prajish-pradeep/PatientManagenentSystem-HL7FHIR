from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, expr

#initialising Spark session
spark = SparkSession.builder \
    .appName("Feedback History Data Processing") \
    .getOrCreate()

#defining input and output bucket paths
INPUT_BUCKET = "gs://feedback_history/*.csv" 
TRANSFORMED_OUTPUT = "gs://output_feedback_history/" 
OUTPUT_BUCKET_MISSING = "gs://missing-data-points/" 

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
print(f"Writing clean data to GCP bucket: {TRANSFORMED_OUTPUT}...")
non_missing_data.write \
    .mode("append") \
    .parquet(TRANSFORMED_OUTPUT)
print(f"Clean data saved to {TRANSFORMED_OUTPUT}")

print("Process completed successfully.")
spark.stop()


'''
gcloud dataproc jobs submit pyspark gs://dataproc_scripts_etl/process_feedback_history.py \
    --cluster=patient-management-system \
    --region=europe-west2 \
    --id=call-feedback-etl
'''


'''
#pub/sub: create a topic
gsutil notification create -t feedback-topic -f json -e OBJECT_FINALIZE gs://output_feedback_history

gcloud pubsub subscriptions create feedback-subscription --topic=feedback-topic
'''

'''
#publish and push
gcloud pubsub topics publish projects/robust-index-446813-f4/topics/call-topic --message "Test message for call-topic" - check if the topic is working
gcloud pubsub subscriptions pull projects/robust-index-446813-f4/subscriptions/call-subscription --auto-ack
'''