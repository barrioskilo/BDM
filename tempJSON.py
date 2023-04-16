import os
import json
import glob
from hdfs import InsecureClient

# Define the directory where your JSON files are located
json_dir = '/Users/anderbarriocampos/Desktop/data/idealista'

# Get a list of all JSON files in the directory
json_files = glob.glob(os.path.join(json_dir, '*.json'))

# Define the HDFS connection parameters
hdfs_host = '10.4.41.36'
hdfs_port = 9870
hdfs_user = 'bdm'
# hdfs_path = '/path/to/hdfs/destination'

# Create a connection to HDFS
client = InsecureClient(f'https://{hdfs_host}:{hdfs_port}', user=hdfs_user)
print('The connection has been properly established.')
print(client.list('./'))

# Define directory path
temp_JSON_dir_path = '/temporal_landing_JSON'

# Check if directory exists
if client.status(temp_JSON_dir_path, strict=False) is None:
    # Create directory
    client.makedirs(temp_JSON_dir_path)
    print(f"Directory {temp_JSON_dir_path} created successfully.")
else:
    print(f"Directory {temp_JSON_dir_path} already exists.")

# Loop through JSON files in local directory
for filename in os.listdir(json_dir):
    if filename.endswith('.json'):
        # Load JSON file
        with open(os.path.join(json_dir, filename), 'r') as f:
            data = json.load(f)

        # Convert JSON data to bytes
        data_bytes = json.dumps(data).encode('utf-8')

        # Upload JSON file to HDFS directory
        hdfs_path = os.path.join(temp_JSON_dir_path, filename)
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(data_bytes)

        print(f"File {filename} uploaded to {hdfs_path} successfully.")
