import json

# Load the JSON file
with open("app_store_reviews_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Basic structure
print("Top-level type:", type(data).__name__)
print("Number of records:", len(data))

# Inspect first record
print("\nFirst record:")
print(data[0])

# Check data types in first record
print("\nFields and data types:")
for key, value in data[0].items():
    print(f"{key}: {type(value).__name__}")

# Check entire dataset for nested objects/arrays
nested_fields = {}

for i, record in enumerate(data):
    for key, value in record.items():
        if isinstance(value, (dict, list)):
            nested_fields.setdefault(key, set()).add(type(value).__name__)

print("\nNested objects/arrays:")
print(nested_fields)

if not nested_fields:
    print("No nested objects or arrays found.")