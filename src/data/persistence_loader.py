import datetime
import json
import happybase
import os
import re

def extract_timestamp_from_filename(filename):
    date_str = re.search(r'\d{4}_\d{2}_\d{2}', filename).group(0)
    # Convert date string to timestamp using datetime module
    timestamp = datetime.datetime.strptime(date_str, '%Y_%m_%d').timestamp()
    return str(int(timestamp))

# Connect to HBase
connection = happybase.Connection('<hbase-master-hostname>', port=9090)

# Create a table for the data source
table_name = 'opendatabcn-income'
connection.create_table(table_name, {'data': {}})

# Get a handle to the table
table = connection.table(table_name)

# Iterate over the CSV files in the temporal landing zone
csv_dir = '/path/to/temporal_landing/temporal_landing_CSV/opendatabcn-income'
for filename in os.listdir(csv_dir):
    if filename.endswith('.csv'):
        with open(os.path.join(csv_dir, filename), 'r') as f:
            # Iterate over the rows in the CSV file
            for line in f:
                # Parse the row and construct the row key
                row_key = f"{table_name}_{line.split(',')[2]}_{filename.split('_')[0]}"

                # Construct a dictionary of column name to value
                data_dict = {
                    'Codi_Districte': line.split(',')[0],
                    'Nom_Districte': line.split(',')[1],
                    'Codi_Barri': line.split(',')[2],
                    'Nom_Barri': line.split(',')[3],
                }

                # Insert the data into HBase
                table.put(row_key, data_dict)

# Iterate over the JSON files in the temporal landing zone
json_dir = '/path/to/temporal_landing/temporal_landing_JSON/idealista'
for filename in os.listdir(json_dir):
    if filename.endswith('.json'):
        with open(os.path.join(json_dir, filename), 'r') as file:
            data = json.load(file)
            for property_dict in data['elementList']:
                # Extract necessary data
                property_code = property_dict['propertyCode']
                neighborhood = property_dict['neighborhood']
                district = property_dict['district']
                timestamp = extract_timestamp_from_filename(filename)
                # Generate key and value
                key = f"idealista_{neighborhood}{timestamp}"
                value = f"{property_code}{district}"
                # Add data to HBase table
                table.put(key.encode(), {'data:property_info'.encode(): value.encode()})
                # You can add more columns as needed. For example:
                # table.put(key.encode(), {
                #     'data:property_info'.encode(): value.encode(),
                #     'data:property_code'.encode(): property_code.encode(),
                #     'data:district'.encode(): district.encode()
                # })