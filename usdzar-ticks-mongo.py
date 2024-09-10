import csv, os
from dotenv import load_dotenv
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

load_dotenv()

# MongoDB connection parameters
mongo_uri = "mongodb+srv://ttawonezvi13:"+os.getenv("MONGO_PASSWORD")+"@cluster0.ev8kj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "usd-zar"
collection_name = "usdzar-ticks"

# CSV file path
csv_file_path = "usdzar_2024_08.csv"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

def parse_date(date_string):
    # Parse the date string format in your CSV
    return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S.%fZ")

def process_csv(file_path):
    documents = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header row
        for row in csvreader:
            # Process each row according to the provided format
            document = {
                "timestamp": parse_date(row[2]),
                "currencyPair": row[1],
                "bid": float(row[3]),
                "ask": float(row[4])
                # 'exness' column is disregarded as per instruction
            }
            documents.append(document)
    return documents

def insert_data(documents):
    try:
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} documents")
    except BulkWriteError as bwe:
        print(f"Bulk write error: {bwe.details}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("Starting data import...")
    files = ["Exness_USDZAR_2023_09.csv", "Exness_USDZAR_2023_10.csv",
             "Exness_USDZAR_2023_11.csv", "Exness_USDZAR_2023_12.csv", "Exness_USDZAR_2024_01.csv",
             "Exness_USDZAR_2024_02.csv", "Exness_USDZAR_2024_03.csv", "Exness_USDZAR_2024_04.csv",
             "Exness_USDZAR_2024_05.csv", "Exness_USDZAR_2024_06.csv", "Exness_USDZAR_2024_07.csv"]
    for file in files:
        documents = process_csv(file)
        print("Inserting data to MongoDB collection...")
        insert_data(documents)
        print("Data import completed.")
    client.close()