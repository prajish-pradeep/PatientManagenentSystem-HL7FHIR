from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import random
import subprocess
from faker import Faker
import re

faker = Faker()

GCP_PROJECT = "robust-index-446813-f4"
GCP_LOCATION = "europe-west2"
GCP_DATASET = "Patient_Management_System"
GCP_FHIR_STORE = "Patient_Resource"
FHIR_API_URL = f"https://healthcare.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/datasets/{GCP_DATASET}/fhirStores/{GCP_FHIR_STORE}/fhir/Endpoint"

#fetching NHS GP data
def fetch_nhs_gp_data(**context):
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
    
    context["ti"].xcom_push(key="nhs_gp_data", value=all_records)

#generating endpoint json
def generate_endpoint_json(record):
    raw_id = record.get('GPPracticeName', '').replace(' ', '').lower()
    transformed_id = re.sub(r'[^a-zA-Z0-9]', '', raw_id)

    endpoint = {
        "resourceType": "Endpoint",
        "id": transformed_id,  
        "text": {
            "status": "generated",
            "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Endpoint resource for {record.get('GPPracticeName')}</div>"
        },
        "identifier": [
            {
                "use": "official",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "MR",
                            "display": "Medical Record Number"
                        }
                    ],
                    "text": "Medical Record Number"
                },
                "system": f"http://{transformed_id}.com",
                "value": f"ENDPOINT-{record.get('PracticeCode')}"
            }
        ],
        "status": random.choice(["active", "suspended", "test"]),
        "connectionType": {
            "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
            "code": "hl7-fhir-rest",
            "display": "HL7 FHIR"
        },
        "name": record.get("GPPracticeName"),
        "managingOrganization": {
            "reference": f"Organization/{record.get('PracticeCode')}",
            "display": record.get("GPPracticeName")
        },
        "contact": [
            {
                "system": "phone",
                "value": record.get("TelephoneNumber", faker.phone_number()),
                "use": "work"
            },
            {
                "system": "email",
                "value": f"{transformed_id}@nhs.scot.co.uk",
                "use": "work"
            }
        ],
        "period": {
            "start": faker.date_this_decade().strftime("%Y-%m-%d"),
            "end": faker.date_between(start_date="today", end_date="+2y").strftime("%Y-%m-%d")
        },
        "address": f"{transformed_id}@nhs.scot.co.uk",
        "header": [record.get("GPPracticeName")],
        "payloadType": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/endpoint-payload-type",
                        "code": "any",
                        "display": "Any"
                    }
                ],
                "text": "Any payload type can be used with this endpoint."
            }
        ]
    }
    return endpoint

#validating all records
def validate_endpoint_data(**context):
    records = context["ti"].xcom_pull(key="nhs_gp_data")
    if not records:
        raise Exception("No data to validate")

    validated_endpoints = []
    for record in records:
        endpoint_json = generate_endpoint_json(record)
        temp_file = "/tmp/endpoint.json"
        with open(temp_file, "w") as file:
            json.dump(endpoint_json, file)

        result = subprocess.run(
            [
                "java",
                "-jar",
                "validator_cli.jar",
                "-version",
                "4.0.1",
                temp_file
            ],
            capture_output=True,
            text=True
        )
        if "FAILURE" in result.stdout:
            raise Exception(f"Validation failed for record {record.get('PracticeCode')}:\n{result.stdout}")
        
        validated_endpoints.append(endpoint_json)
    
    context["ti"].xcom_push(key="validated_endpoints", value=validated_endpoints)

#debugging access token
def debug_access_token():
    token_result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True
    )
    if token_result.returncode != 0:
        raise Exception(f"Failed to fetch access token:\n{token_result.stderr}")
    return token_result.stdout.strip()

#sending all validated data to GCP
def send_all_to_gcp(**context):
    validated_endpoints = context["ti"].xcom_pull(key="validated_endpoints")
    if not validated_endpoints:
        raise Exception("No validated data to send")

    access_token = debug_access_token()
    for endpoint in validated_endpoints:
        id_ = endpoint["id"]
        print(f"Uploading Endpoint ID {id_}...")
        result = subprocess.run([
            "curl",
            "-X", "PUT",
            "-H", f"Authorization: Bearer {access_token}",
            "-H", "Content-Type: application/fhir+json",
            "-d", json.dumps(endpoint),
            f"{FHIR_API_URL}/{id_}"
        ], capture_output=True, text=True)
        
        if result.returncode != 0 or "error" in result.stdout.lower():
            print(f"Error uploading Endpoint {id_}:\n{result.stdout}")
            raise Exception(f"Failed to upload Endpoint {id_}")
        print(f"Endpoint {id_} successfully uploaded: {result.stdout}")

#airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    "insert_endpoints_with_validation",
    default_args=default_args,
    description="Fetch NHS GP data, validate Endpoint resources, and send to GCP",
    schedule_interval="0 0 * * *", 
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_nhs_gp_data",
        python_callable=fetch_nhs_gp_data
    )

    validate_data_task = PythonOperator(
        task_id="validate_endpoint_data",
        python_callable=validate_endpoint_data,
        provide_context=True,
    )

    send_data_task = PythonOperator(
        task_id="send_endpoints_to_gcp",
        python_callable=send_all_to_gcp,
        provide_context=True,
    )

    fetch_data_task >> validate_data_task >> send_data_task