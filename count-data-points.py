from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from dotenv import load_dotenv

load_dotenv()

# InfluxDB connection parameters
url = "http://localhost:8086"
token = os.getenv("ALL_ACCESS_TOKEN")
org = "University of Cape Town"
bucket = "usd-zar"

# Create a client object
client = InfluxDBClient(url=url, token=token, org=org)

# Create a query API instance
query_api = client.query_api()

# Flux query to count all data points
query = f'''
from(bucket:"{bucket}")
  |> range(start: 0)
  |> count()
'''

# Execute the query
result = query_api.query(query)

# Extract and print the count
# total_count = 0
# for table in result:
#     for record in table.records:
#         total_count += record.row["bid"]

# print(f"Total number of data points in the bucket: {total_count}")
print(result)
# Close the client
client.close()