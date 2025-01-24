from faker import Faker
import random
from datetime import datetime, timezone
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

# Function to generate Condition resources
def generate_condition_data(nhs_data, patients_per_code=1):
    """Generate Condition resources linked to existing Patient resources."""
    conditions = []
    global_patient_counter = 0  # Initialize patient counter for IDs
    global_condition_counter = 0  # Initialize condition counter

    # SNOMED CT codes and descriptions for conditions
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
        "unconfirmed", "provisional", "differential", "confirmed", "refuted", "entered-in-error"
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

    for record in nhs_data:
        practice_code = record.get("PracticeCode")

        for _ in range(patients_per_code):
            # Increment patient counter
            global_patient_counter += 1
            if global_patient_counter > 100:
                global_patient_counter = 1

            # Generate Patient ID
            patient_id = f"P{practice_code}-{global_patient_counter:02d}"

            # Increment condition counter
            global_condition_counter += 1

            # Generate Condition ID
            condition_id = f"C{global_condition_counter:05d}"

            # Select random values from standardized sets
            clinical_status = random.choice(clinical_status_options)
            verification_status = random.choice(verification_status_options)
            category = random.choice(category_options)
            severity = random.choice(severity_options)
            condition_code = random.choice(condition_codes)

            # Create Condition resource
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
                "recordedDate": datetime.now(timezone.utc).isoformat(),  # Ensure timezone is included
                "note": [
                    {
                        "text": "This is a randomly generated condition for testing purposes."
                    }
                ]
            }

            # Ensure clinicalStatus matches FHIR constraints for abated conditions
            if clinical_status in ["inactive", "resolved", "remission"]:
                condition["abatementDateTime"] = faker.date_between(start_date="-1y", end_date="today").isoformat()
            else:
                condition.pop("abatementDateTime", None)

            conditions.append(condition)

    return conditions

# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=1)

# Generate Condition resources
conditions = generate_condition_data(nhs_gp_data, patients_per_code=1)

# Save Conditions to a JSON file
with open("condition_resources.json", "w") as file:
    json.dump(conditions, file, indent=4)

print("Condition resources saved to 'condition_resources.json'.")

# Validation:
# java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/condition_resources.json

# Upload:
# curl -X POST \
#   -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
#   -H "Content-Type: application/fhir+json" \
#   -d @condition_resources.json \
#   "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Condition/"