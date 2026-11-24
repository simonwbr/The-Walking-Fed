from pymongo import MongoClient

client = MongoClient(
    "mongodb://admin:password123@localhost:27017/?authSource=admin"
)

db = client["open_data"]

collection = db["drinking_water"]

print("Connected to MongoDB!")