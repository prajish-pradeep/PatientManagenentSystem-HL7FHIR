from faker import Faker
import json
import requests

faker = Faker()

# Function to fetch data from the NHS API
def fetch_nhs_gp_data(limit=10):
    """Fetch GP practice data from the NHS API."""
    url = "https://www.opendata.nhs.scot/api/3/action/datastore_search"
    params = {
        "resource_id": "0d2e258a-1451-4af1-a7e5-e8327994fa55",  # Replace with the actual resource ID for GP data
        "limit": limit
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json().get("result", {}).get("records", [])
    else:
        print(f"Failed to fetch data from NHS API: {response.status_code}")
        return []

# Function to generate organization data
def generate_organization_data(nhs_data):
    """Generate Organization resources from NHS GP data."""
    organizations = []
    for record in nhs_data:
        # Combine address lines
        address_lines = list(filter(None, [
            record.get("AddressLine1"),
            record.get("AddressLine2"),
            record.get("AddressLine3")
        ]))

        # Set city: use AddressLine4 if available, otherwise fallback to AddressLine3
        city = record.get("AddressLine4") if record.get("AddressLine4") else record.get("AddressLine3")

        # Build organization resource
        organization = {
            "resourceType": "Organization",
            "id": str(record.get("PracticeCode")),  # Ensure id is a string
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
                    "value": str(record.get("PracticeCode"))  # Ensure value is a string
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
                        "given": [faker.first_name()]  # Ensure given is an array
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
        organizations.append(organization)
    return organizations

# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=1)

# Generate Organization resources
organizations = generate_organization_data(nhs_gp_data)

# Save to a JSON file
with open("organization_resources.json", "w") as file:
    json.dump(organizations, file, indent=2)

print("Organization resources saved to 'organization_resources.json'.")


#java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/organization_resources.json

'''
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  -H "Content-Type: application/fhir+json" \
  -d @organization_resources.json \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Organization"
'''

'''
curl -X DELETE \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Organization/10002"
'''