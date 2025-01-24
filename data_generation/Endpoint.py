from faker import Faker
import requests
import random
import json

faker = Faker()

# Function to fetch NHS GP data
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

# Function to generate Endpoint data
def generate_endpoint_data(nhs_data):
    """Generate Endpoint resources linked to the Organization data."""
    endpoints = []
    for record in nhs_data:
        # Generate Endpoint resource linked to the Organization
        endpoint = {
            "resourceType": "Endpoint",
            "id": f"{record.get('GPPracticeName').replace(' ', '').lower()}",
            "text": {
                "status": "generated",
                "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">Endpoint resource for {record.get('GPPracticeName')}</div>"
            },
            "identifier": [
                {
                    "use": "official",
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "MR",  # Valid code from IdentifierType ValueSet
                                "display": "Medical Record Number"
                            }
                        ],
                        "text": "Medical Record Number"
                    },
                    "system": f"http://{record.get('GPPracticeName').replace(' ', '').lower()}.com",
                    "value": f"ENDPOINT-{record.get('PracticeCode')}"
                }
            ],
            "status": random.choice(["active", "suspended", "test"]),
            "connectionType": {
                "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                "code": "hl7-fhir-rest",
                "display": "HL7 FHIR"
            },
            "name": record.get("GPPracticeName"),
            "managingOrganization": {
                "reference": f"Organization/{record.get('PracticeCode')}",
                "display": record.get("GPPracticeName")
            },
            "contact": [
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
            "period": {
                "start": faker.date_this_decade().strftime("%Y-%m-%d"),
                "end": faker.date_between(start_date="today", end_date="+2y").strftime("%Y-%m-%d")
            },
            "address": f"{record.get('GPPracticeName').replace(' ', '').lower()}@nhs.scot.co.uk",
            "header": [record.get("GPPracticeName")],
            "payloadType": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/endpoint-payload-type",
                            "code": "any",
                            "display": "Any"
                        }
                    ],
                    "text": "Any payload type can be used with this endpoint."
                }
            ]
        }
        endpoints.append(endpoint)
    return endpoints

# Fetch NHS GP practice data
nhs_gp_data = fetch_nhs_gp_data(limit=10)

# Generate Endpoint resources linked to the Organizations
endpoints = generate_endpoint_data(nhs_gp_data)

# Save to a JSON file
with open("endpoint_resources.json", "w") as file:
    json.dump(endpoints, file, indent=4)

print("Endpoint resources saved to 'endpoint_resources.json'.")


'''
curl -X PUT \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  -H "Content-Type: application/fhir+json" \
  -d @endpoint_resources.json \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Endpoint/muirheadmedicalcentre"
'''

#java -jar /Users/prajishpradeep/Downloads/validator_cli.jar -version 4.0.1 /Users/prajishpradeep/Patient_Management_System/endpoint_resources.json

'''
curl -X DELETE \
  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  "https://healthcare.googleapis.com/v1/projects/robust-index-446813-f4/locations/europe-west2/datasets/Patient_Management_System/fhirStores/Patient_Resource/fhir/Endpoint/muirheadmedicalcentre"
'''
