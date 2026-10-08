import json

with open("app_store_reviews_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)

total_records = len(data)

with_response = sum(
    1
    for record in data
    if isinstance(record.get("developerResponse"), dict)
)

without_response = total_records - with_response

print("Total records:", total_records)
print("With developerResponse:", with_response)
print("Without developerResponse:", without_response)