from faker import Faker
import json
import requests
import random
import re

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

# Function to generate PractitionerRole data
def generate_practitioner_role_data(nhs_data):
    """Generate PractitionerRole resources from NHS GP data."""
    practitioner_roles = []

    # Define valid codes for PractitionerRole and Specialty
    practitioner_roles_codes = [
        {"code": "doctor", "system": "http://terminology.hl7.org/CodeSystem/practitioner-role", "display": "Doctor"},
        {"code": "nurse", "system": "http://terminology.hl7.org/CodeSystem/practitioner-role", "display": "Nurse"},
        {"code": "pharmacist", "system": "http://terminology.hl7.org/CodeSystem/practitioner-role", "display": "Pharmacist"},
        {"code": "researcher", "system": "http://terminology.hl7.org/CodeSystem/practitioner-role", "display": "Researcher"},
        {"code": "teacher", "system": "http://terminology.hl7.org/CodeSystem/practitioner-role", "display": "Teacher/educator"},
        {"code": "ict", "system": "http://terminology.hl7.org/CodeSystem/practitioner-role", "display": "ICT professional"}
    ]

    specialties = [
        {"code": "408443003", "system": "http://snomed.info/sct", "display": "General medical practice"},
        {"code": "394802001", "system": "http://snomed.info/sct", "display": "General medicine"},
        {"code": "419772000", "system": "http://snomed.info/sct", "display": "Family practice"},
        {"code": "394577000", "system": "http://snomed.info/sct", "display": "Anesthetics"},
        {"code": "394579002", "system": "http://snomed.info/sct", "display": "Cardiology"},
        {"code": "394814009", "system": "http://snomed.info/sct", "display": "General practice"},
        {"code": "408462000", "system": "http://snomed.info/sct", "display": "Burns care"},
        {"code": "394585009", "system": "http://snomed.info/sct", "display": "Obstetrics and gynecology"},
        {"code": "394812008", "system": "http://snomed.info/sct", "display": "Dental medicine specialties"},
        {"code": "408444009", "system": "http://snomed.info/sct", "display": "Dental-General dental practice"},
        {"code": "394582007", "system": "http://snomed.info/sct", "display": "Dermatology"},
        {"code": "408475000", "system": "http://snomed.info/sct", "display": "Diabetic medicine"},
        {"code": "410005002", "system": "http://snomed.info/sct", "display": "Dive medicine"},
        {"code": "394583002", "system": "http://snomed.info/sct", "display": "Endocrinology"},
        {"code": "394808002", "system": "http://snomed.info/sct", "display": "Genito-urinary medicine"},
        {"code": "394811001", "system": "http://snomed.info/sct", "display": "Geriatric medicine"},
        {"code": "394586005", "system": "http://snomed.info/sct", "display": "Gynecology"},
        {"code": "408468001", "system": "http://snomed.info/sct", "display": "Learning disability"},
        {"code": "408459003", "system": "http://snomed.info/sct", "display": "Pediatric cardiology"},
        {"code": "394607009", "system": "http://snomed.info/sct", "display": "Pediatric dentistry"},
        {"code": "408450004", "system": "http://snomed.info/sct", "display": "Sleep studies"},
        {"code": "408476004", "system": "http://snomed.info/sct", "display": "Surgery-Bone and marrow transplantation"},
        {"code": "394612005", "system": "http://snomed.info/sct", "display": "Urology"}
    ]

    for record in nhs_data:
        # Combine address lines
        address_lines = list(filter(None, [
            record.get("AddressLine1"),
            record.get("AddressLine2"),
            record.get("AddressLine3")
        ]))

        # Set city: use AddressLine4 if available, otherwise fallback to AddressLine3
        city = record.get("AddressLine4") if record.get("AddressLine4") else record.get("AddressLine3")

        # Randomly select a practitioner role and specialty
        chosen_role = random.choice(practitioner_roles_codes)
        chosen_specialty = random.choice(specialties)

        # Build PractitionerRole resource
        practitioner_role = {
            "resourceType": "PractitionerRole",
            "text": {
                "status": "generated",
                "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Practitioner role for {record.get('GPPracticeName')} with specialty {chosen_specialty['display']} at organization {record.get('GPPracticeName')}.</div>"
            },
            "id": str(record.get("PracticeCode")),  # Ensure id is a string
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
                    "system": f"http://{re.sub(r'[^a-zA-Z0-9-]', '', record.get('GPPracticeName', '').replace(' ', '').lower())}.com",
                    "value": f"ROLE-{record.get('PracticeCode')}"
                }
            ],
            "active": True,
            "period": {
                "start": faker.date_this_decade().strftime("%Y-%m-%d"),
                "end": faker.date_between(start_date="today", end_date="+2y").strftime("%Y-%m-%d")
            },
            "practitioner": {
                "reference": f"Practitioner/{faker.uuid4()}",
                "display": faker.name()
            },
            "organization": {
                "reference": f"Organization/{record.get('PracticeCode')}",
                "display": record.get("GPPracticeName")
            },
            "code": [
                {
                    "coding": [
                        {
                            "system": chosen_role["system"],
                            "code": chosen_role["code"],
                            "display": chosen_role["display"]
                        }
                    ],
                    "text": chosen_role["display"]
                }
            ],
            "specialty": [
                {
                    "coding": [
                        {
                            "system": chosen_specialty["system"],
                            "code": chosen_specialty["code"],
                            "display": chosen_specialty["display"]
                        }
                    ],
                    "text": chosen_specialty["display"]
                }
            ],
            "endpoint": [
                {
                    "reference": f"Endpoint/{re.sub(r'[^a-zA-Z0-9-]', '', record.get('GPPracticeName', '').replace(' ', '').lower())}",
                    "display": f"{record.get('GPPracticeName')} Endpoint"
                }
            ]
        }
        practitioner_roles.append(practitioner_role)
    return practitioner_roles

# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=1)

# Generate PractitionerRole resources
practitioner_roles = generate_practitioner_role_data(nhs_gp_data)

# Save to a JSON file
with open("practitioner_role_resources.json", "w") as file:
    json.dump(practitioner_roles, file, indent=4)

print("PractitionerRole resources saved to 'practitioner_role_resources.json'.")

#java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/practitioner_role_resources.json

'''
curl -X PUT \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  -H "Content-Type: application/fhir+json" \
  -d @practitioner_role_resources.json \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/PractitionerRole/10002"
'''

'''
curl -X DELETE \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Organization/10002"
'''