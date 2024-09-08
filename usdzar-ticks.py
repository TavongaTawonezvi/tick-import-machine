import influxdb_client, os, time, csv
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

token = os.getenv("ALL_ACCESS_TOKEN")
org = "University of Cape Town"
url = "http://localhost:8086"

csv_file_path = "usd_zar_ticks.csv"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

bucket = "usd-zar"
BATCH_SIZE = 1000


def create_point(row):
    timestamp = datetime.fromisoformat(row['timestamp'].replace("Z", "+00:00"))
    return Point("forex_ticks") \
        .tag("currency_pair", "USD/ZAR") \
        .field("bid", float(row['bid'])) \
        .field("ask", float(row['ask'])) \
        .time(timestamp)
