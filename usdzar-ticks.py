import influxdb_client, os, time, csv
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

token = os.getenv("ALL_ACCESS_TOKEN")
org = "University of Cape Town"
url = "http://localhost:8086"

#csv_file_path = "usdzar_2024_08.csv"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

bucket = "usd-zar"
BATCH_SIZE = 1000


def create_point(row):
    timestamp = datetime.fromisoformat(row['Timestamp'].replace("Z", "+00:00"))
    return Point("forex_ticks") \
        .tag("currency_pair", "USD/ZAR") \
        .field("bid", float(row['Bid'])) \
        .field("ask", float(row['Ask'])) \
        .time(timestamp)


def process_csv(csv_file_path):
    points = []
    total_processed = 0

    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            points.append(create_point(row))

            if len(points) >= BATCH_SIZE:
                write_api.write(bucket=bucket, record=points)
                total_processed += len(points)
                print(f"Processed {total_processed} ticks")
                points = []

        # Write any remaining points
        if points:
            write_api.write(bucket=bucket, record=points)
            total_processed += len(points)

    print(f"Total ticks processed: {total_processed}")

# Run the process
files  = ["Exness_USDZAR_2023_08.csv", "Exness_USDZAR_2023_09.csv", "Exness_USDZAR_2023_10.csv", "Exness_USDZAR_2023_11.csv", "Exness_USDZAR_2023_12.csv", "Exness_USDZAR_2024_01.csv", "Exness_USDZAR_2024_02.csv", "Exness_USDZAR_2024_03.csv", "Exness_USDZAR_2024_04.csv", "Exness_USDZAR_2024_05.csv", "Exness_USDZAR_2024_06.csv", "Exness_USDZAR_2024_07.csv"]
try:
    for file in files:
        process_csv(file)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    write_client.close()

print("Process completed.")