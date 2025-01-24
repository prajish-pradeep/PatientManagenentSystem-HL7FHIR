from faker import Faker
import json
import random
import re
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

# Generate Practitioner data
def generate_practitioner_data(nhs_data):
    """Generate Practitioner resources using NHS GP data."""
    practitioners = []

    # Valid qualification codes
    qualification_codes = [
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "MD", "display": "Doctor of Medicine"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "RN", "display": "Registered Nurse"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "DO", "display": "Doctor of Osteopathy"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "PharmD", "display": "Doctor of Pharmacy"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "CNP", "display": "Certified Nurse Practitioner"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "NP", "display": "Nurse Practitioner"},
        {"system": "http://terminology.hl7.org/CodeSystem/v2-0360", "code": "PA", "display": "Physician Assistant"}
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

        # Randomly select a qualification
        chosen_qualification = random.choice(qualification_codes)

        # Build Practitioner resource
        practitioner = {
            "resourceType": "Practitioner",
            "text": {
                "status": "generated",
                "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Practitioner record.</div>"
            },
            "id": f"{record.get('PracticeCode')}-practitioner1",
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
        practitioners.append(practitioner)
    return practitioners

# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=1)

# Generate Practitioner resources
practitioner_data = generate_practitioner_data(nhs_gp_data)

# Save to a JSON file
with open("practitioner_resources.json", "w") as file:
    json.dump(practitioner_data, file, indent=4)

print("Practitioner resources saved to 'practitioner_resources.json'.")

#java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/practitioner_resources.json
