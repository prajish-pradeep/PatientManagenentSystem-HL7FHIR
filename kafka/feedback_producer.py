import requests
import random
from faker import Faker
from confluent_kafka import Producer

faker = Faker()

#function to read the kafka client configuration from client.properties
def read_config():
    config = {}
    try:
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.split("=", 1)
                    config[parameter.strip()] = value.strip()
    except FileNotFoundError:
        raise FileNotFoundError("The client.properties file is missing.")
    except ValueError:
        raise ValueError("Invalid format in client.properties file.")
    
    required_keys = ["bootstrap.servers", "security.protocol", "sasl.mechanisms", "sasl.username", "sasl.password"]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required key in client.properties: {key}")
    
    return config

#fetch data from NHS API
def fetch_nhs_gp_data():
    url = "https://www.opendata.nhs.scot/api/3/action/datastore_search"
    params = {
        "resource_id": "0d2e258a-1451-4af1-a7e5-e8327994fa55",
        "limit": 1000
    }
    all_records = []
    offset = 0

    while True:
        params["offset"] = offset
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json().get("result", {})
            records = data.get("records", [])
            all_records.extend(records)
            if len(records) < params["limit"]:
                break
            offset += params["limit"]
        else:
            raise Exception(f"Failed to fetch data from NHS API: {response.status_code}")
    
    return all_records

#generating patient ids from NHS Data
def generate_patient_ids(nhs_data, max_patients_per_practice=100):
    patient_ids = []
    for record in nhs_data:
        practice_code = record.get("PracticeCode")
        for i in range(1, max_patients_per_practice + 1):
            patient_id = f"P{practice_code}-{i:02d}"
            patient_ids.append(patient_id)

    return patient_ids

#generating feeback history data
def generate_feedback_history(patient_ids, num_records=20000):
    feedback_types = [
        "complaint", 
        "praise", 
        "suggestion", 
        "inquiry", 
        "technical issue", 
        "service request"
    ]
    healthcare_feedback = [
        "The doctor was very attentive and explained everything clearly.",
        "The wait time was too long, but the service was excellent.",
        "The nurse was extremely kind and professional.",
        "I had trouble understanding the instructions provided over the call.",
        "The process was seamless, and the staff were very supportive.",
        "The issue I raised wasn't resolved, but the staff were polite.",
        "The webchat was convenient, but response times were a bit slow.",
        "The department was very responsive and handled my query effectively.",
        "I appreciate how the staff prioritized my urgent request.",
        "The feedback process was straightforward and easy to navigate.",
        "The system for scheduling appointments needs improvement.",
        "The resolution provided was helpful, but it took longer than expected.",
        "The staff's professionalism and empathy made a big difference.",
        "I faced technical difficulties while accessing the service online.",
        "The overall experience was positive, but there’s room for improvement."
        ]
    
    feedback_modes = ["call", "webchat"]
    departments = ["cardiology", "neurology", "orthopedics", "pediatrics", "general"]
    call_centres = [
        "West Regional Centre - Caledonia House - GLASGOW",
        "West Regional Centre - Lumina Building - GLASGOW",
        "West Regional Centre - Aurora House - GLASGOW",
        "East Regional Centre - South Queensferry",
        "East Regional Centre - DUNDEE",
        "North Regional Centre - ABERDEEN"
    ]
    for i in range(1, num_records + 1):
        satisfaction_score = random.randint(5, 10)
        professionalism_score = random.randint(5, 10)
        response_time_score = random.randint(5, 10)
        resolution_score = random.randint(5, 10)
        feedback_comment = random.choice(healthcare_feedback)
        overall_score = (satisfaction_score + professionalism_score + response_time_score + resolution_score) / 4
        resolution_provided = random.choice(["Yes", "No"])
        yield {
            "feedback_id": f"FB-{i:05d}",
            "patient_id": random.choice(patient_ids),
            "timestamp": faker.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
            "feedback_type": random.choice(feedback_types),
            "feedback_mode": random.choice(feedback_modes),
            "department": random.choice(departments),
            "call_centre": random.choice(call_centres),
            "comments": feedback_comment,
            "satisfaction_score": satisfaction_score,
            "professionalism_score": professionalism_score,
            "response_time_score": response_time_score,
            "resolution_score": resolution_score,
            "overall_score": overall_score,
            "resolution_provided": resolution_provided
        }

#converting feedback history data to csv row
def convert_to_csv_row(record):
    return f'{record["feedback_id"]},{record["patient_id"]},{record["timestamp"]},{record["feedback_type"]},{record["feedback_mode"]},{record["department"]},"{record["call_centre"]}","{record["comments"]}",{record["satisfaction_score"]},{record["professionalism_score"]},{record["response_time_score"]},{record["resolution_score"]},{record["overall_score"]},{record["resolution_provided"]}'

def produce(topic, config, call_data):
    producer = Producer(config)

    #send only the data rows
    for record in call_data:
        #convert record to CSV and send it
        csv_row = convert_to_csv_row(record)
        try:
            producer.produce(topic, value=csv_row)
            print(f"Sent: {csv_row}")
        except Exception as e:
            print(f"Failed to send message: {e}")

    producer.flush()
    print("All data sent to Kafka.")

#main function
def main():
    print("Reading Kafka client configuration...")
    config = read_config()

    print("Fetching NHS GP data...")
    nhs_data = fetch_nhs_gp_data()
    patient_ids = generate_patient_ids(nhs_data)

    print(f"Generated {len(patient_ids)} patient IDs.")
    feedback_data = generate_feedback_history(patient_ids, num_records=20000)

    topic = "feedback_history"
    produce(topic, config, feedback_data)

if __name__ == "__main__":
    main()