import requests
import json
import time
import random
from faker import Faker
from confluent_kafka import Producer

faker = Faker()

#function to read the Kafka client configuration from client.properties
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
    
    #ensure required keys are present
    required_keys = ["bootstrap.servers", "security.protocol", "sasl.mechanisms", "sasl.username", "sasl.password"]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required key in client.properties: {key}")
    
    return config

#NHS API Data Fetcher
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


#generating call history data
def generate_call_history(patient_ids, num_records=20000):
    call_types = ["query", "complaint", "emergency", "appointment", "follow-up"]
    resolutions = ["resolved", "unresolved", "pending"]
    departments = ["cardiology", "neurology", "orthopedics", "pediatrics", "general"]
    call_notes = [
    "Booked an appointment for the patient.",
    "Informed the patient to stay home and monitor symptoms, advised to call back if condition worsens.",
    "Provided instructions to take prescribed medications as directed.",
    "Assisted the patient in accessing their online health records.",
    "Transferred the call to the appropriate department for further assistance.",
    "Explained the next steps for their treatment plan.",
    "Advised the patient on lifestyle changes to support recovery.",
    "Provided information about clinic hours and location.",
    "Helped the employee with technical support for accessing patient data.",
    "Clarified insurance coverage details for a specific procedure.",
    "Informed the patient that test results are pending and provided a timeline for follow-up.",
    "Escalated the call to a supervisor for resolution of a complex issue.",
    "Reassured the patient and scheduled a follow-up consultation with their doctor.",
    "Advised on proper wound care and hygiene to prevent infection.",
    "Shared information on available vaccination programs.",
    "Helped the patient book transportation to the clinic for an appointment."
]
    call_centres = [
    "West Regional Centre - Caledonia House - GLASGOW",
    "West Regional Centre - Lumina Building - GLASGOW",
    "West Regional Centre - Aurora House - GLASGOW",
    "East Regional Centre - South Queensferry",
    "East Regional Centre - DUNDEE",
    "North Regional Centre - ABERDEEN"
]
    for i in range(1, num_records + 1):
        yield {
            "call_id": f"CL-{i:03d}",
            "patient_id": random.choice(patient_ids),
            "timestamp": faker.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
            "call_type": random.choice(call_types),
            "resolution_status": random.choice(resolutions),
            "department": random.choice(departments),
            "call_duration": random.randint(1, 60),  # Duration in minutes
            "notes": random.choice(call_notes),
            "follow_up_required": random.choice(["Yes", "No"]),
            "satisfaction_score": random.randint(1, 5),
            "call_centre": random.choice(call_centres)  
        }

#converting call history data to csv
def convert_to_csv_row(record):
    return f'{record["call_id"]},{record["patient_id"]},{record["timestamp"]},{record["call_type"]},{record["resolution_status"]},{record["department"]},{record["call_duration"]},"{record["notes"]}",{record["follow_up_required"]},{record["satisfaction_score"]},{record["call_centre"]}'

def produce(topic, config, call_data):
    producer = Producer(config)

    csv_header = "call_id,patient_id,timestamp,call_type,resolution_status,department,call_duration,notes,follow_up_required,satisfaction_score,call_centre"

    try:
        producer.produce(topic, value=csv_header)
        print(f"Sent header: {csv_header}")
    except Exception as e:
        print(f"Failed to send header: {e}")

    for record in call_data:
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

    #generating call history data
    call_data = generate_call_history(patient_ids, num_records=20000)

    #sending call history data to Kafka
    topic = "call_history"  # Kafka topic name
    produce(topic, config, call_data)

if __name__ == "__main__":
    main()