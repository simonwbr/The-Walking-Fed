
import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB Docker
client = MongoClient(
    "mongodb://admin:password123@localhost:27017/?authSource=admin"
)

# Select database and collection
db = client["open_data"]
collection = db["fruit_locations_france"]

# Read CSV using semicolon separator
df = pd.read_csv("/Users/victorkindberg/The-Walking-Fed/data/TestCSV.csv", sep=",")

# Extract latitude and longitude from OSM Point
# coordinates = df["OSM Point"].str.split(",", n=1, expand=True)

# df["latitude"] = pd.to_numeric(coordinates[0].str.strip(), errors="coerce")
# df["longitude"] = pd.to_numeric(coordinates[1].str.strip(), errors="coerce")

# Convert CSV rows into MongoDB documents
documents = df.where(pd.notna(df), None).to_dict("records")

# Insert in batches
batch_size = 1000
total_inserted = 0

for i in range(0, len(documents), batch_size):
    batch = documents[i:i + batch_size]
    result = collection.insert_many(batch)
    total_inserted += len(result.inserted_ids)

print(f"Successfully imported {total_inserted} fruit locations!")

client.close()
