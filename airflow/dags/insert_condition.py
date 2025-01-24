from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import random
from faker import Faker
from datetime import timezone
import subprocess

faker = Faker()

GCP_PROJECT = "robust-index-446813-f4"
GCP_LOCATION = "europe-west2"
GCP_DATASET = "Patient_Management_System"
GCP_FHIR_STORE = "Patient_Resource"
FHIR_API_URL = f"https://healthcare.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/datasets/{GCP_DATASET}/fhirStores/{GCP_FHIR_STORE}/fhir/Condition"

#function to fetch data from the NHS API
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

#function to generate Condition resources
def generate_condition_data(nhs_data, patients_per_code=10):
    """Generate Condition resources linked to existing Patient resources."""
    conditions = []
    global_patient_counter = 0  
    global_condition_counter = 0

    condition_codes = [
        {"code": "473011001", "display": "Allergic condition (finding)"},
        {"code": "372130007", "display": "Malignant neoplasm of skin (disorder)"},
        {"code": "25064002", "display": "Headache (finding)"},
        {"code": "22298006", "display": "Myocardial infarction (disorder)"},
        {"code": "63741006", "display": "Fungal infection of lung (disorder)"},
        {"code": "56717001", "display": "Tuberculosis (disorder)"},
        {"code": "16541001", "display": "Yellow fever (disorder)"},
        {"code": "267102003", "display": "Sore throat (finding)"}
    ]

    clinical_status_options = [
        "active", "recurrence", "relapse", "inactive", "remission", "resolved"
    ]
    verification_status_options = [
        "unconfirmed", "provisional", "differential", "confirmed", "refuted"
    ]
    category_options = [
        {"code": "problem-list-item", "display": "Problem List Item"},
        {"code": "encounter-diagnosis", "display": "Encounter Diagnosis"}
    ]
    severity_options = [
        {"code": "255604002", "display": "Mild"},
        {"code": "1255665007", "display": "Moderate"},
        {"code": "24484000", "display": "Severe"}
    ]

    #generating condition resources
    for record in nhs_data:
        practice_code = record.get("PracticeCode")

        for _ in range(patients_per_code):
            global_patient_counter += 1
            if global_patient_counter > 100:
                global_patient_counter = 1

            patient_id = f"P{practice_code}-{global_patient_counter:02d}"
            global_condition_counter += 1
            condition_id = f"C{global_condition_counter:05d}"

            clinical_status = random.choice(clinical_status_options)
            verification_status = random.choice(verification_status_options)
            category = random.choice(category_options)
            severity = random.choice(severity_options)
            condition_code = random.choice(condition_codes)

            condition = {
                "resourceType": "Condition",
                "id": condition_id,
                "text": {
                    "status": "generated",
                    "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Condition resource for {patient_id}</div>"
                },
                "clinicalStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                            "code": clinical_status,
                            "display": clinical_status.capitalize()
                        }
                    ]
                },
                "verificationStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                            "code": verification_status,
                            "display": verification_status.capitalize()
                        }
                    ]
                },
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                                "code": category["code"],
                                "display": category["display"]
                            }
                        ]
                    }
                ],
                "severity": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": severity["code"],
                            "display": severity["display"]
                        }
                    ]
                },
                "code": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": condition_code["code"],
                            "display": condition_code["display"]
                        }
                    ]
                },
                "subject": {
                    "reference": f"Patient/{patient_id}",
                    "display": f"Linked to Patient {patient_id}"
                },
                "onsetDateTime": faker.date_this_decade().isoformat(),
                "recordedDate": datetime.now(timezone.utc).isoformat(),
                "note": [
                    {
                        "text": "This is a randomly generated condition for testing purposes."
                    }
                ]
            }
            conditions.append(condition)

    return conditions

#validating condition data
def validate_condition_data(**context):
    nhs_gp_data = context["ti"].xcom_pull(key="nhs_gp_data")
    if not nhs_gp_data:
        raise Exception("No data to validate")

    conditions = generate_condition_data(nhs_gp_data)
    context["ti"].xcom_push(key="validated_conditions", value=conditions)

#uploading condition data to GCP
def send_conditions_to_gcp(**context):
    validated_conditions = context["ti"].xcom_pull(key="validated_conditions")
    if not validated_conditions:
        raise Exception("No validated data to send")

    access_token = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True
    ).stdout.strip()

    for condition in validated_conditions:
        response = requests.put(
            f"{FHIR_API_URL}/{condition['id']}",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/fhir+json"
            },
            json=condition
        )
        if response.status_code != 200:
            print(f"Failed to upload Condition ID {condition['id']}: {response.status_code} - {response.text}")
        else:
            print(f"Successfully uploaded Condition ID {condition['id']}")

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
    "insert_conditions_with_validation",
    default_args=default_args,
    description="Fetch NHS GP data, generate Condition resources, validate, and send to GCP",
    schedule_interval="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_nhs_gp_data",
        python_callable=fetch_nhs_gp_data
    )

    validate_task = PythonOperator(
        task_id="validate_condition_data",
        python_callable=validate_condition_data,
        provide_context=True,
    )

    send_task = PythonOperator(
        task_id="send_conditions_to_gcp",
        python_callable=send_conditions_to_gcp,
        provide_context=True,
    )

    fetch_task >> validate_task >> send_task