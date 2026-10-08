import json
from pprint import pprint

with open("app_store_reviews_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for record in data:
    if "developerResponse" in record:
        print("Found developerResponse:")
        pprint(record["developerResponse"])
        print("\nFull record:")
        pprint(record)
        break
else:
    print("No developerResponse found.")