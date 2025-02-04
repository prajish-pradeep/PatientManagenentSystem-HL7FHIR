from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import random
import subprocess
from faker import Faker
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

faker = Faker()

GCP_PROJECT = "robust-index-446813-f4"
GCP_LOCATION = "europe-west2"
GCP_DATASET = "Patient_Management_System"
GCP_FHIR_STORE = "Patient_Resource"
FHIR_API_URL = f"https://healthcare.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/datasets/{GCP_DATASET}/fhirStores/{GCP_FHIR_STORE}/fhir/Patient"

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

#generating patient JSON
def generate_patient_data(n, nhs_data):
    language_mapping = {
        "en": "English",
        "es": "Spanish",
        "fr": "French",
        "de": "German"
    }

    marital_status_mapping = {
        "M": "Married",
        "S": "Never Married",
        "D": "Divorced",
        "W": "Widowed",
        "L": "Legally Separated"
    }

    contact_relationship_mapping = {
        "E": "Employer",
        "N": "Next-of-Kin",
        "F": "Family",
        "S": "Spouse",
        "O": "Other",
        "R": "Relative"
    }

    patients = []
    global_patient_counter = 0

    patients_per_code = n // len(nhs_data)
    for record in nhs_data:
        for _ in range(patients_per_code):
            global_patient_counter += 1

            if global_patient_counter > 100:
                global_patient_counter = 1

            address_lines = list(filter(None, [
                record.get("AddressLine2"),
                record.get("AddressLine3")
            ]))
            city = record.get("AddressLine4") if record.get("AddressLine4") else record.get("AddressLine3")

            selected_language_code = random.choice(list(language_mapping.keys()))
            selected_marital_status_code = random.choice(list(marital_status_mapping.keys()))
            selected_relationship_code = random.choice(list(contact_relationship_mapping.keys()))

            start_date = faker.date_this_decade()
            end_date = faker.date_between(start_date=start_date + timedelta(days=1))
            practitioner_id = f"{record.get('PracticeCode')}-practitioner{random.randint(1, 10)}"

            patient = {
                "resourceType": "Patient",
                "id": f"P{record.get('PracticeCode')}-{global_patient_counter:02d}",
                "text": {
                    "status": "generated",
                    "div": f"<div xmlns='http://www.w3.org/1999/xhtml'>Patient record</div>"
                },
                "identifier": [
                    {
                        "use": "usual",
                        "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR", "display": "Medical Record Number"}]},
                        "system": "https://nhs.co.uk",
                        "value": f"MRN-{global_patient_counter:02d}"
                    }
                ],
                "active": True,
                "name": [
                    {
                        "use": "official",
                        "family": faker.last_name(),
                        "given": [faker.first_name()],
                        "prefix": [random.choice(["Mr.", "Ms.", "Dr."])]
                    }
                ],
                "telecom": [
                    {
                        "system": "phone",
                        "value": faker.phone_number(),
                        "use": "home"
                    },
                    {
                        "system": "email",
                        "value": f"{faker.first_name().lower()}.{faker.last_name().lower()}@gmail.com",
                        "use": "work"
                    }
                ],       
                "gender": random.choice(["male", "female", "other", "unknown"]),
                "birthDate": faker.date_of_birth(minimum_age=0, maximum_age=100).strftime("%Y-%m-%d"),
                "address": [
                    {
                        "use": "home",
                        "line": address_lines,
                        "city": city,
                        "state": record.get("GPCluster"),
                        "postalCode": record.get("Postcode"),
                        "country": "United Kingdom"
                    }
                ],
                "maritalStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
                            "code": selected_marital_status_code,
                            "display": marital_status_mapping[selected_marital_status_code]
                        }
                    ],
                    "text": marital_status_mapping[selected_marital_status_code]
                },
                "multipleBirthBoolean": random.choice([True, False]),
                "contact": [
                    {
                        "relationship": [
                            {
                                "coding": [
                                    {
                                        "system": "http://terminology.hl7.org/CodeSystem/v2-0131",
                                        "code": selected_relationship_code,
                                        "display": contact_relationship_mapping[selected_relationship_code]
                                    }
                                ]
                            }
                        ],
                        "name": {
                            "family": faker.last_name(),
                            "given": [faker.first_name()],
                            "prefix": [random.choice(["Mr.", "Ms.", "Dr."])]
                        },
                        "telecom": [
                            {
                                "system": "phone",
                                "value": faker.phone_number(),
                                "use": "mobile"
                            }
                        ],
                        "address": {
                            "line": address_lines,
                            "city": city,
                            "state": record.get("GPCluster"),
                            "postalCode": record.get("Postcode"),
                            "country": "United Kingdom"
                        },
                        "gender": random.choice(["male", "female", "other"]),
                        "organization": {
                            "reference": f"Organization/{record.get('PracticeCode')}",
                            "display": record.get("GPPracticeName", "NHS Practice")
                        },
                        "period": {
                            "start": start_date.strftime("%Y-%m-%d"),
                            "end": end_date.strftime("%Y-%m-%d")
                        }
                    }
                ],
                "communication": [
                    {
                        "language": {
                            "coding": [
                                {
                                    "system": "urn:ietf:bcp:47",
                                    "code": selected_language_code,
                                    "display": language_mapping[selected_language_code]
                                }
                            ],
                            "text": language_mapping[selected_language_code]
                        },
                        "preferred": random.choice([True, False])
                    }
                ],
                "generalPractitioner": [
                    {"reference": f"Practitioner/{practitioner_id}", "display": f"Practitioner from {record.get('GPPracticeName', 'NHS Practice')}"}
                ],
                "managingOrganization": {
                    "reference": f"Organization/{record.get('PracticeCode')}",
                    "display": record.get("GPPracticeName", "NHS Practice")
                }
            }

            #validating resourceType
            if "resourceType" not in patient or patient["resourceType"] != "Patient":
                raise Exception(f"Invalid Patient resource (missing resourceType): {patient}")
            
            patients.append(patient)
    return patients

def validate_patient_data(**context):
    records = context["ti"].xcom_pull(key="nhs_gp_data")
    if not records:
        raise Exception("No data to validate")

    patients = generate_patient_data(88700, records)
    validated_records = []
    batch_size = 100
    for i in range(0, len(patients), batch_size):
        batch = patients[i:i + batch_size]
        for patient in batch:
            try:
                temp_file = f"/tmp/patient_{patient['id']}.json"
                with open(temp_file, "w") as f:
                    json.dump(patient, f)
                
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
                    text=True,
                    timeout=6000
                )
                if "FAILURE" not in result.stdout:
                    validated_records.append(patient)
                else:
                    print(f"Validation failed for {patient['id']}: {result.stdout}")
            except subprocess.TimeoutExpired:
                print(f"Validation timed out for {patient['id']}")
            except Exception as e:
                print(f"Unexpected error validating {patient['id']}: {e}")
    
    context["ti"].xcom_push(key="validated_records", value=validated_records)

def send_to_gcp(**context):
    validated_records = context["ti"].xcom_pull(key="validated_records")
    if not validated_records:
        raise Exception("No validated data to send")

    #retrieving access token
    access_token = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True
    ).stdout.strip()

    #checkpoint file to track uploaded resources
    checkpoint_file = "/tmp/uploaded_patients.txt"
    uploaded_ids = set()

    #loading previously uploaded IDs from checkpoint file
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            uploaded_ids = set(line.strip() for line in f.readlines())

    def upload_patient(patient):
        patient_id = patient["id"]
        if patient_id in uploaded_ids:
            return f"Patient {patient_id} already uploaded, skipping."

        try:
            response = requests.put(
                f"{FHIR_API_URL}/{patient_id}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/fhir+json"
                },
                json=patient
            )
            if response.status_code == 200:
                with open(checkpoint_file, "a") as f:
                    f.write(patient_id + "\n")
                return f"Successfully uploaded Patient ID {patient_id}"
            else:
                return f"Failed to upload Patient ID {patient_id}: {response.status_code} - {response.text}"
        except requests.exceptions.RequestException as e:
            return f"Error uploading Patient ID {patient_id}: {e}"

    #using ThreadPoolExecutor for parallel uploads
    max_workers = 10
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_patient = {executor.submit(upload_patient, patient): patient for patient in validated_records}

        for future in as_completed(future_to_patient):
            result = future.result()
            print(result)  # Log upload result

#airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}
with DAG(
    "insert_patients_with_validation",
    default_args=default_args,
    description="Fetch NHS GP data, generate Patient resources, validate, and send to GCP",
    schedule_interval="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_nhs_gp_data",
        python_callable=fetch_nhs_gp_data
    )

    validate_task = PythonOperator(
        task_id="validate_patient_data",
        python_callable=validate_patient_data,
        provide_context=True,
    )

    send_task = PythonOperator(
        task_id="send_to_gcp",
        python_callable=send_to_gcp,
        provide_context=True,
    )

    fetch_task >> validate_task >> send_task