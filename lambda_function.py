import base64
import zlib
import json
from pymongo import MongoClient

# MongoDB settings
mongo_connection_string = "mongodb+srv://admin:TeleglobalPune2001@cluster0.vpx0ozf.mongodb.net/?retryWrites=true&w=majority"
mongo_database_name = "test-db"
mongo_collection_name = "vpc_flow_logs"

# Hardcoded log group name
log_group = "dr-vpc-flow-logs-cloudwatch-log-grp"

def lambda_handler(event, context):
    try:
        if not log_group:
            print("Invalid or missing log group name.")
            return

        # Extract the base64-encoded and compressed log data
        log_data = event['awslogs']['data']
        decoded_data = base64.b64decode(log_data)
        decompressed_data = zlib.decompress(decoded_data, zlib.MAX_WBITS | 16)

        # Convert the decompressed data to a string
        log_events_str = decompressed_data.decode('utf-8')
        log_events = log_events_str.splitlines()

        # Process and upload logs to MongoDB
        upload_logs_to_mongodb(log_events)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

def upload_logs_to_mongodb(logs):
    client = MongoClient(mongo_connection_string)
    db = client[mongo_database_name]
    collection = db[mongo_collection_name]

    for log in logs:
        try:
            log_dict = json.loads(log)
            # Insert the log dictionary into MongoDB
            collection.insert_one(log_dict)
        except json.JSONDecodeError:
            print(f"Invalid JSON log format: {log}")
