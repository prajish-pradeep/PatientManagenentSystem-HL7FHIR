from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import subprocess
from faker import Faker
import random
import re

faker = Faker()

#google Cloud configuration
GCP_PROJECT = "robust-index-446813-f4"
GCP_LOCATION = "europe-west2"
GCP_DATASET = "Patient_Management_System"
GCP_FHIR_STORE = "Patient_Resource"
FHIR_API_URL = f"https://healthcare.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/datasets/{GCP_DATASET}/fhirStores/{GCP_FHIR_STORE}/fhir/Practitioner"

#fetching NHS GP data
def fetch_nhs_gp_data(**context):
    """Fetch NHS GP practice data."""
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

#generating practitioner json
def generate_practitioner_json(record):
    qualification_codes = [
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "MD", "display": "Doctor of Medicine"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "RN", "display": "Registered Nurse"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "DO", "display": "Doctor of Osteopathy"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "PharmD", "display": "Doctor of Pharmacy"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "CNP", "display": "Certified Nurse Practitioner"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "NP", "display": "Nurse Practitioner"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "PA", "display": "Physician Assistant"}
    ]
    chosen_qualification = random.choice(qualification_codes)

    address_lines = list(filter(None, [
        record.get("AddressLine1"),
        record.get("AddressLine2"),
        record.get("AddressLine3")
    ]))
    city = record.get("AddressLine4") if record.get("AddressLine4") else record.get("AddressLine3")

    practitioner = {
        "resourceType": "Practitioner",
        "text": {
            "status": "generated",
            "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Practitioner record for {record.get('GPPracticeName')}.</div>"
        },
        "id": f"{record.get('PracticeCode')}-practitioner",
        "identifier": [
            {
                "use": "official",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "PRN",
                            "display": "Provider number"
                        }
                    ],
                    "text": "Provider number"
                },
                "system": f"https://{re.sub(r'[^a-zA-Z0-9-]', '', record.get('GPPracticeName', '').replace(' ', '').lower())}.com",
                "value": f"PR-{record.get('PracticeCode')}"
            }
        ],
        "active": faker.boolean(),
        "name": [
            {
                "family": faker.last_name(),
                "given": [faker.first_name()],
                "prefix": [random.choice(["Dr.", "Mr.", "Mrs.", "Ms."])],
                "suffix": [random.choice(["MD", "DO", "RN", "PharmD"])]
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": record.get("TelephoneNumber", faker.phone_number()),
                "use": "work"
            },
            {
                "system": "email",
                "value": f"{record.get('GPPracticeName').replace(' ', '').lower()}@nhs.scot.co.uk",
                "use": "work"
            }
        ],
        "gender": random.choice(["male", "female", "other", "unknown"]),
        "birthDate": faker.date_of_birth(minimum_age=25, maximum_age=65).strftime("%Y-%m-%d"),
        "address": [
            {
                "line": address_lines,
                "city": city,
                "state": record.get("GPCluster"),
                "postalCode": record.get("Postcode"),
                "country": "United Kingdom"
            }
        ],
        "qualification": [
            {
                "identifier": [
                    {
                        "use": "official",
                        "system": "https://nhs.com.uk/qualification-id",
                        "value": f"QUAL-{faker.random_int(min=10000, max=99999)}"
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": chosen_qualification["system"],
                            "code": chosen_qualification["code"],
                            "display": chosen_qualification["display"]
                        }
                    ],
                    "text": chosen_qualification["display"]
                },
                "period": {
                    "start": faker.date_this_decade().strftime("%Y-%m-%d"),
                    "end": faker.date_between(start_date="today", end_date="+5y").strftime("%Y-%m-%d")
                },
                "issuer": {
                    "reference": f"Organization/{record.get('PracticeCode')}",
                    "display": record.get("GPPracticeName")
                }
            }
        ],
        "communication": [
            {
                "coding": [
                    {
                        "system": "urn:ietf:bcp:47",
                        "code": "en",
                        "display": "English"
                    }
                ]
            }
        ]
    }
    return practitioner

#validate practitioner data
def validate_practitioner_data(**context):
    records = context["ti"].xcom_pull(key="nhs_gp_data")
    if not records:
        raise Exception("No data to validate")

    validated_practitioners = []
    for record in records:
        practitioner_json = generate_practitioner_json(record)
        temp_file = "/tmp/practitioner.json"
        with open(temp_file, "w") as file:
            json.dump(practitioner_json, file)

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
        
        validated_practitioners.append(practitioner_json)
    
    context["ti"].xcom_push(key="validated_practitioners", value=validated_practitioners)

#send validated practitioners to GCP
def send_validated_practitioners_to_gcp(**context):
    validated_practitioners = context["ti"].xcom_pull(key="validated_practitioners")
    if not validated_practitioners:
        raise Exception("No validated data to send")

    access_token = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True
    ).stdout.strip()

    for practitioner in validated_practitioners:
        id_ = practitioner["id"]
        subprocess.run([
            "curl",
            "-X", "PUT",
            "-H", f"Authorization: Bearer {access_token}",
            "-H", "Content-Type: application/fhir+json",
            "-d", json.dumps(practitioner),
            f"{FHIR_API_URL}/{id_}"
        ], check=True)
        print(f"Record ID {id_} successfully sent to GCP.")

#airflow DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    "insert_practitioners_with_validation",
    default_args=default_args,
    description="Fetch NHS GP data, validate Practitioner resources, and send to GCP",
    schedule_interval="42 22 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_all_nhs_gp_data",
        python_callable=fetch_nhs_gp_data
    )

    validate_data_task = PythonOperator(
        task_id="validate_all_practitioner_data",
        python_callable=validate_practitioner_data,
        provide_context=True,
    )

    send_data_task = PythonOperator(
        task_id="send_all_validated_practitioners_to_gcp",
        python_callable=send_validated_practitioners_to_gcp,
        provide_context=True,
    )

    fetch_data_task >> validate_data_task >> send_data_task
    
