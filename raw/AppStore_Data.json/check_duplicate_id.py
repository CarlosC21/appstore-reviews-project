import json
from collections import Counter

# Load JSON file
with open("app_store_reviews_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)

print("Total records:", len(data))

# Extract _id values
ids = [record.get("_id") for record in data]

# Count occurrences of each ID
id_counts = Counter(ids)

# Keep IDs that occur more than once
duplicates = {
    record_id: count
    for record_id, count in id_counts.items()
    if count > 1
}

print("Unique _id values:", len(id_counts))
print("Duplicate _id values:", len(duplicates))

if duplicates:
    print("\nDuplicate IDs:")
    for record_id, count in duplicates.items():
        print(f"{record_id}: {count}")
else:
    print("\nNo duplicate _id values found.")