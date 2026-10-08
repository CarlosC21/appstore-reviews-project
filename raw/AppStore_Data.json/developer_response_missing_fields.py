import json

with open("app_store_reviews_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)

fields = ["body", "id", "modified"]

for field in fields:
    missing = 0

    for record in data:
        response = record.get("developerResponse")

        if isinstance(response, dict):
            if response.get(field) is None:
                missing += 1

    print(f"{field}: {missing} missing values")