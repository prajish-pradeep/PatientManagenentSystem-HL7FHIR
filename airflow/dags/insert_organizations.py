from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import subprocess
from faker import Faker

faker = Faker()

GCP_PROJECT = "robust-index-446813-f4"
GCP_LOCATION = "europe-west2"
GCP_DATASET = "Patient_Management_System"
GCP_FHIR_STORE = "Patient_Resource"
FHIR_API_URL = f"https://healthcare.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/datasets/{GCP_DATASET}/fhirStores/{GCP_FHIR_STORE}/fhir/Organization"

#fetch NHS GP data
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

#generating organization JSON
def generate_organization_json(record):
    address_lines = list(filter(None, [
        record.get("AddressLine1"),
        record.get("AddressLine2"),
        record.get("AddressLine3")
    ]))
    city = record.get("AddressLine4") if record.get("AddressLine4") else record.get("AddressLine3")

    organization = {
        "resourceType": "Organization",
        "id": str(record.get("PracticeCode")),
        "text": {
            "status": "generated",
            "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Organization resource for {record.get('GPPracticeName')}</div>"
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
                "system": f"http://{record.get('GPPracticeName').replace(' ', '').lower()}.com",
                "value": str(record.get("PracticeCode"))
            }
        ],
        "active": True,
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "prov",
                        "display": "Healthcare Provider"
                    }
                ],
                "text": "Healthcare Provider"
            }
        ],
        "name": record.get("GPPracticeName"),
        "alias": ["NHS Scotland"],
        "contact": [
            {
                "purpose": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/contactentity-type",
                            "code": "ADMIN",
                            "display": "Administrative"
                        }
                    ],
                    "text": "Administrative Contact"
                },
                "name": {
                    "family": faker.last_name(),
                    "given": [faker.first_name()]
                },
                "telecom": [
                    {
                        "system": "email",
                        "value": f"{record.get('GPPracticeName').replace(' ', '').lower()}@nhs.scot.co.uk",
                        "use": "work"
                    },
                    {
                        "system": "phone",
                        "value": record.get("TelephoneNumber", faker.phone_number()),
                        "use": "work"
                    }
                ],
                "address": {
                    "line": address_lines,
                    "city": city,
                    "state": record.get("GPCluster"),
                    "postalCode": record.get("Postcode"),
                    "country": "United Kingdom"
                }
            }
        ]
    }
    return organization

#validating all records
def validate_organization_data(**context):
    records = context["ti"].xcom_pull(key="nhs_gp_data")
    if not records:
        raise Exception("No data to validate")

    validated_records = []
    for record in records:
        organization_json = generate_organization_json(record)
        temp_file = "/tmp/organization.json"
        with open(temp_file, "w") as file:
            json.dump(organization_json, file)

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
        
        validated_records.append(organization_json)
    
    context["ti"].xcom_push(key="validated_records", value=validated_records)

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
    validated_records = context["ti"].xcom_pull(key="validated_records")
    if not validated_records:
        raise Exception("No validated data to send")

    access_token = debug_access_token()
    for organization_json in validated_records:
        id_ = organization_json["id"]
        subprocess.run([
            "curl",
            "-X", "PUT",
            "-H", f"Authorization: Bearer {access_token}",
            "-H", "Content-Type: application/fhir+json",
            "-d", json.dumps(organization_json),
            f"{FHIR_API_URL}/{id_}"
        ], check=True)
        print(f"Record ID {id_} successfully sent to GCP.")

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
    "insert_organizations_with_validation",
    default_args=default_args,
    description="Fetch NHS GP data, validate, and send to GCP in one go",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_all_nhs_gp_data",
        python_callable=fetch_nhs_gp_data
    )

    validate_data_task = PythonOperator(
        task_id="validate_all_organization_data",
        python_callable=validate_organization_data,
        provide_context=True,
    )

    send_data_task = PythonOperator(
        task_id="send_all_to_gcp",
        python_callable=send_all_to_gcp,
        provide_context=True,
    )

    fetch_data_task >> validate_data_task >> send_data_task