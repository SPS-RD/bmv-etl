import json
import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


# MongoDB connection URL

def get_mongo_client() -> MongoClient:
    mongo_host = os.environ.get('MONGO_HOST')
    mongo_port = os.environ.get('MONGO_PORT')
    mongo_user = os.environ.get('MONGO_USERNAME')
    mongo_pwd = os.environ.get('MONGO_PASSWORD')
    mongo_client = MongoClient(f"mongodb://{mongo_user}:{mongo_pwd}@{mongo_host}:{mongo_port}")
    return mongo_client


client = get_mongo_client()
db = client['bmv']  # replace with your database name
collection = db['promozioni']  # replace with your collection name


def insert_json(filename):
    with open(filename, 'r') as f:
        file_data = json.load(f)

    # Convert JSON data to MongoDB's BSON format
    if isinstance(file_data, list):
        collection.insert_many(file_data)
    else:
        collection.insert_one(file_data)


insert_json('collections/promozioni_0.json')  # replace with your json file name
