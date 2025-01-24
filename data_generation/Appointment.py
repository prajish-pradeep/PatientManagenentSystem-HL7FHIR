from faker import Faker
import random
from datetime import datetime, timezone, timedelta
import json
import requests

faker = Faker()

# Function to fetch data from the NHS API
def fetch_nhs_gp_data(limit=10):
    """Fetch GP practice data from the NHS API."""
    url = "https://www.opendata.nhs.scot/api/3/action/datastore_search"
    params = {
        "resource_id": "0d2e258a-1451-4af1-a7e5-e8327994fa55",
        "limit": limit
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json().get("result", {}).get("records", [])
    else:
        print(f"Failed to fetch data from NHS API: {response.status_code}")
        return []

# Function to generate Appointment resources
def generate_appointment_data(nhs_data, patients_per_code=1):
    """Generate Appointment resources linked to existing Patient and Practitioner resources."""
    appointments = []
    global_patient_counter = 0

    # Enhanced descriptions for appointments
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
        {"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "REF", "display": "Referrer"},
        {"system": "http://terminology.hl7.org/CodeSystem/participant-type", "code": "translator", "display": "Translator"},
        {"system": "http://terminology.hl7.org/CodeSystem/participant-type", "code": "emergency", "display": "Emergency"}
    ]

    participant_status_options = ["accepted", "declined", "tentative", "needs-action"]

    for record in nhs_data:
        practice_code = record.get("PracticeCode")

        for _ in range(patients_per_code):
            # Increment patient counter
            global_patient_counter += 1
            if global_patient_counter > 100:
                global_patient_counter = 1

            # Generate IDs
            patient_id = f"P{practice_code}-{global_patient_counter:02d}"
            appointment_id = f"A{global_patient_counter:05d}"
            practitioner_id = f"{practice_code}-practitioner{random.randint(1, 10)}"

            # Select random values for participant type and status
            participant_type_patient = random.choice(participant_type_options)
            participant_type_practitioner = random.choice(participant_type_options)
            participant_status = random.choice(participant_status_options)

            # Create Appointment resource
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
                "description": random.choice(appointment_descriptions),  # Use random choice for dynamic descriptions
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

# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=1)

# Generate Appointment resources
appointments = generate_appointment_data(nhs_gp_data, patients_per_code=1)

# Save Appointments to a JSON file
with open("appointment_resources.json", "w") as file:
    json.dump(appointments, file, indent=4)

print("Appointment resources saved to 'appointment_resources.json'.")

# Validation:
# java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/appointment_resources.json

'''
curl -X PUT \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  -H "Content-Type: application/fhir+json" \
  -d @appointment_resources.json \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Appointment/A00001"
'''