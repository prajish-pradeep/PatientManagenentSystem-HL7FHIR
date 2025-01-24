from faker import Faker
import random
from datetime import datetime, timedelta
import json
import requests

faker = Faker()

# Fetch NHS GP data from the API
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

# Generate Patient data
def generate_patient_data(n, nhs_data):
    """Generate Organization resources from NHS GP data."""
    organizations = []
    for record in nhs_data:
        address_lines = list(filter(None, [
            record.get("AddressLine2"),
            record.get("AddressLine3")
        ]))

        # Set city: use AddressLine4 if available, otherwise fallback to AddressLine3
        city = record.get("AddressLine4") if record.get("AddressLine4") else record.get("AddressLine3")

    """Generate n random Patient resources in FHIR-compliant format."""
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
    for i, record in enumerate(nhs_data[:n]):
        # Select random values for codes
        selected_language_code = random.choice(list(language_mapping.keys()))
        selected_marital_status_code = random.choice(list(marital_status_mapping.keys()))
        selected_relationship_code = random.choice(list(contact_relationship_mapping.keys()))

        # Generate valid period
        start_date = faker.date_this_decade()
        end_date = faker.date_between(start_date=start_date + timedelta(days=1))

        # Random Practitioner ID between 1 and 10
        practitioner_id = f"{record.get('PracticeCode')}-practitioner{random.randint(1, 10)}"

        # Build Patient resource
        patient = {
            "resourceType": "Patient",
            "id": f"P{record.get('PracticeCode')}-{i+1:02d}",
            "text": {
                "status": "generated",
                "div": f"<div xmlns='http://www.w3.org/1999/xhtml'>Patient record</div>"
            },
            "identifier": [
                {
                    "use": "usual",
                    "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR", "display": "Medical Record Number"}]},
                    "system": "https://nhs.co.uk",
                    "value": f"MRN-{i+1}"
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
                    "value": faker.email(),
                    "use": "work"
                }
            ],
            "gender": random.choice(["male", "female", "other", "unknown"]),
            "birthDate": faker.date_of_birth(minimum_age=0, maximum_age=100).strftime("%Y-%m-%d"),
            "deceasedBoolean": random.choice([True, False]),
            "address": [
                {
                    "use": "home",
                    "type": "both",
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
                    "gender": random.choice(["male", "female", "other", "unknown"]),
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
        patients.append(patient)
    return patients


# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=1)

# Generate Patient resources
patients = generate_patient_data(5, nhs_gp_data)

# Save to a JSON file
with open("patient_resources.json", "w") as file:
    json.dump(patients, file, indent=4)

print("Patient resources saved to 'patient_resources.json'.")

#java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/patient_resources.json

'''
curl -X PUT \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  -H "Content-Type: application/fhir+json" \
  -d @patient_resources.json \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Patient/P10002-01"
'''