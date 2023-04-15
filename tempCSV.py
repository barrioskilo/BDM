import os
import csv
import glob
from hdfs import InsecureClient

# Define the directory where your CSV files are located
csv_dir = '/Users/anderbarriocampos/Desktop/data/opendatabcn-income'

# Get a list of all CSV files in the directory
csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))

# Define the HDFS connection parameters
hdfs_host = '10.4.41.36'
hdfs_port = 9870
hdfs_user = 'bdm'
#hdfs_path = '/path/to/hdfs/destination'

# Create a connection to HDFS
client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
print('The conection has been properly established.')
#print(client.list('./'))

# Define directory path
temp_CSV_dir_path = '/temporal_landing_CSV'

# Check if directory exists
if client.status(temp_CSV_dir_path, strict=False) is None:
    # Create directory
    client.makedirs(temp_CSV_dir_path)
    print(f"Directory {temp_CSV_dir_path} created successfully.")
else:
    print(f"Directory {temp_CSV_dir_path} already exists.")

# Loop through CSV files in local directory
for filename in os.listdir(csv_dir):
    if filename.endswith('.csv'):
        # Load CSV file
        with open(os.path.join(csv_dir, filename), 'r') as f:
            csv_reader = csv.reader(f)
            # Convert CSV data to bytes
            data_bytes = bytes('\n'.join([','.join(row) for row in csv_reader]), encoding='utf-8')

        # Upload CSV file to HDFS directory
        hdfs_path = os.path.join(temp_CSV_dir_path, filename)
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(data_bytes)

        print(f"File {filename} uploaded to {hdfs_path} successfully.")
