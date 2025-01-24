from confluent_kafka import Consumer
from google.cloud import storage
import time

#function to read Kafka client configuration from client.properties
def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

#function to consume messages and send batches to GCP Storage
def consume(topic, config, bucket_name, batch_size=1000, rotate_interval_ms=60000, end_timeout=10):
    # Set consumer group ID and offset
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"  # Ensure starting from the beginning

    consumer = Consumer(config)
    consumer.subscribe([topic])

    #initialize GCP Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    messages = [] 
    file_count = 0
    headers = "call_id,patient_id,timestamp,call_type,resolution_status,department,call_duration,notes,follow_up_required,satisfaction_score,call_centre"
    last_rotation_time = time.time()  
    last_message_time = time.time()  

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_message_time > end_timeout:
                    break  
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            csv_line = msg.value().decode('utf-8')
            messages.append(csv_line)
            last_message_time = time.time() 

            current_time = time.time()
            if len(messages) >= batch_size or (current_time - last_rotation_time) * 1000 >= rotate_interval_ms:
                file_name = f"call_history_batch_{file_count}.csv"
                file_content = headers + "\n" + "\n".join(messages)
                try:
                    blob = bucket.blob(file_name)
                    blob.upload_from_string(file_content, content_type="text/csv")
                    print(f'Successfully uploaded {file_name} to GCP bucket {bucket_name} with {len(messages)} lines')
                except Exception as e:
                    print(f'Failed to upload {file_name} to GCP bucket: {e}')

                messages = []
                file_count += 1
                last_rotation_time = current_time

    except KeyboardInterrupt:
        pass
    finally:
        if messages:
            file_name = f"call_history_batch_{file_count}.csv"
            file_content = headers + "\n" + "\n".join(messages)
            try:
                blob = bucket.blob(file_name)
                blob.upload_from_string(file_content, content_type="text/csv")
                print(f'Successfully uploaded final {file_name} to GCP bucket {bucket_name} with {len(messages)} lines')
            except Exception as e:
                print(f'Failed to upload {file_name} to GCP bucket: {e}')

        consumer.close()

def main():
    config = read_config()
    topic = "call_history"
    bucket_name = "patient_call_history"

    consume(topic, config, bucket_name)

if __name__ == "__main__":
    main()