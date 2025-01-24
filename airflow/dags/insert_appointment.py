from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import json
import requests
import random
import subprocess
from faker import Faker

faker = Faker()

#gcp onfiguration
GCP_PROJECT = "robust-index-446813-f4"
GCP_LOCATION = "europe-west2"
GCP_DATASET = "Patient_Management_System"
GCP_FHIR_STORE = "Patient_Resource"
FHIR_API_URL = f"https://healthcare.googleapis.com/v1/projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/datasets/{GCP_DATASET}/fhirStores/{GCP_FHIR_STORE}/fhir/Appointment"

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

def generate_appointment_data(nhs_data, patients_per_code=5):
    appointments = []
    global_patient_counter = 0
    global_appointment_counter = 0

    appointment_descriptions = [
        "Routine check-up appointment",
        "Follow-up consultation",
        "Initial assessment for new symptoms",
        "Post-surgery recovery review",
        "Annual health screening",
        "Specialist consultation for ongoing care",
        "Vaccination appointment",
        "Diagnostic test discussion",
        "Medication management session",
        "Pre-operative assessment"
    ]

    participant_type_options = [
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "ADM", "display": "Admitter"},
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "ATND", "display": "Attender"},
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "CALLBCK", "display": "Callback contact"},
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "CON", "display": "Consultant"},
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "DIS", "display": "Discharger"},
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "ESC", "display": "Escort"},
        {"system": "http://terminology.hl7.org/CodeSystem/participant-type", "code": "translator", "display": "Translator"},
        {"system": "http://terminology.hl7.org/CodeSystem/participant-type", "code": "emergency", "display": "Emergency"}
    ]

    participant_status_options = ["accepted", "declined", "tentative", "needs-action"]

    for record in nhs_data:
        practice_code = record.get("PracticeCode")

        for _ in range(patients_per_code):

            global_patient_counter += 1
            if global_patient_counter > 100:
                global_patient_counter = 1

            global_appointment_counter += 1

            patient_id = f"P{practice_code}-{global_patient_counter:02d}"
            appointment_id = f"A{global_appointment_counter:05d}"
            practitioner_id = f"{practice_code}-practitioner{random.randint(1, 10)}"

            participant_type_patient = random.choice(participant_type_options)
            participant_type_practitioner = random.choice(participant_type_options)
            participant_status = random.choice(participant_status_options)

            #creating appointment resource
            appointment = {
                "resourceType": "Appointment",
                "id": appointment_id,
                "text": {
                    "status": "generated",
                    "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Appointment resource for Patient {patient_id}</div>"
                },
                "identifier": [
                    {
                        "use": "official",
                        "system": "https://healthcare.example.com/appointment-id",
                        "value": appointment_id
                    }
                ],
                "status": random.choice(["proposed", "pending", "booked", "arrived", "fulfilled", "cancelled", "noshow", "entered-in-error", "checked-in", "waitlist"]),
                "description": random.choice(appointment_descriptions),
                "start": (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30))).isoformat(),
                "end": (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30), minutes=30)).isoformat(),
                "created": datetime.now(timezone.utc).isoformat(),
                "minutesDuration": 30,
                "participant": [
                    {
                        "type": [
                            {
                                "coding": [
                                    {
                                        "system": participant_type_patient["system"],
                                        "code": participant_type_patient["code"],
                                        "display": participant_type_patient["display"]
                                    }
                                ]
                            }
                        ],
                        "actor": {
                            "reference": f"Patient/{patient_id}",
                            "display": f"Patient {patient_id}"
                        },
                        "required": "required",
                        "status": participant_status
                    },
                    {
                        "type": [
                            {
                                "coding": [
                                    {
                                        "system": participant_type_practitioner["system"],
                                        "code": participant_type_practitioner["code"],
                                        "display": participant_type_practitioner["display"]
                                    }
                                ]
                            }
                        ],
                        "actor": {
                            "reference": f"Practitioner/{practitioner_id}",
                            "display": f"Practitioner {practitioner_id}"
                        },
                        "required": "required",
                        "status": participant_status
                    }
                ]
            }
            appointments.append(appointment)

    return appointments

#validating appointment data
def validate_appointment_data(**context):
    records = context["ti"].xcom_pull(key="nhs_gp_data")
    if not records:
        raise Exception("No data to validate")

    appointments = generate_appointment_data(records, patients_per_code=10)
    validated_records = []
    for appointment in appointments:
        temp_file = f"/tmp/appointment_{appointment['id']}.json"
        with open(temp_file, "w") as f:
            json.dump(appointment, f)

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
            print(f"Validation failed for record {appointment['id']}:\n{result.stdout}")
            continue
        validated_records.append(appointment)
    
    context["ti"].xcom_push(key="validated_records", value=validated_records)

#sending validated data to GCP
def send_to_gcp(**context):
    validated_records = context["ti"].xcom_pull(key="validated_records")
    if not validated_records:
        raise Exception("No validated data to send")

    access_token = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True
    ).stdout.strip()

    batch_size = 100
    for i in range(0, len(validated_records), batch_size):
        batch = validated_records[i:i + batch_size]
        for appointment in batch:
            id_ = appointment["id"]
            response = requests.put(
                f"{FHIR_API_URL}/{id_}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/fhir+json"
                },
                json=appointment
            )
            if response.status_code != 200:
                print(f"Failed to upload Appointment ID {id_}: {response.status_code} - {response.text}")
                continue
            print(f"Successfully uploaded Appointment ID {id_}")

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
    "insert_appointments_with_validation",
    default_args=default_args,
    description="Fetch NHS GP data, generate Appointment resources, validate, and send to GCP",
    schedule_interval="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_nhs_gp_data",
        python_callable=fetch_nhs_gp_data
    )

    validate_task = PythonOperator(
        task_id="validate_appointment_data",
        python_callable=validate_appointment_data,
        provide_context=True,
    )

    send_task = PythonOperator(
        task_id="send_to_gcp",
        python_callable=send_to_gcp,
        provide_context=True,
    )

    fetch_task >> validate_task >> send_task