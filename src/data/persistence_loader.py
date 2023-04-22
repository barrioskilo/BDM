import datetime
import json
import happybase
import logging
import os
import re
from hdfs import InsecureClient


class PersistenceLoader:
    def __init__(self, host, hbase_port, hdfs_port, hdfs_user, temporal_landing_dir, temporal_landing_csv,
                 temporal_landing_json, logger):
        self.connection = None
        self.host = host
        self.hbase_port = int(hbase_port)
        self.hdfs_port = hdfs_port
        self.hdfs_user = hdfs_user
        self.logger = logger or logging.getLogger(__name__)
        self.temporal_landing_dir = temporal_landing_dir.replace('\\', '/')
        self.temporal_landing_csv = temporal_landing_csv.replace('\\', '/')
        self.temporal_landing_json = temporal_landing_json.replace('\\', '/')
        self.hdfs_client = InsecureClient(f'http://{self.host}:{self.hdfs_port}', user=self.hdfs_user)
        self.logger.info(f"Connection to HDFS has been established successfully.")
        self.connect()

    def connect(self):
        self.logger.info(f"Connecting to HBase at {self.host}:{self.hbase_port}")
        con = happybase.Connection(self.host, self.hbase_port)
        con.open()
        self.connection = con
        self.logger.info("Successfully connected to HBase")

    def close(self):
        if self.connection is not None:
            self.logger.info("Closing HBase connection")
            self.connection.close()
            self.connection = None
            self.logger.info("HBase connection closed")

    def extract_timestamp_from_filename(file_name):
        date_str = re.search(r'\d{4}\d{2}\d{2}', file_name).group(0)
        # Convert date string to timestamp using datetime module
        ts = datetime.datetime.strptime(date_str, '%Y_%m_%d').timestamp()
        return str(int(ts))

    def create_table(self, table_name):
        # Check if the table exists
        if table_name.encode() in self.connection.tables():
            self.logger.info(f"Table '{table_name}' already exists.")
        else:
            # Create the table with 'data' and 'metadata' column families
            self.connection.create_table(table_name, {'data': {}, 'metadata': {}})
            self.logger.info(f"Table '{table_name}' created.")

    def load_opendatabcn_income(self):

        # Create a table for the data source
        table_name = 'opendatabcn-income'
        self.create_table(table_name)

        # Get a handle to the table
        table = self.connection.table(table_name)

        # Iterate over the CSV files in the temporal landing zone
        csv_dir = os.path.join(self.temporal_landing_dir, self.temporal_landing_csv, table_name).replace('\\', '/')
        filenames = self.hdfs_client.list(csv_dir)
        for filename in filenames:
            if filename.endswith('.csv'):
                with self.hdfs_client.read(os.path.join(csv_dir, filename).replace('\\', '/'), encoding='utf-8') as f:
                    # Iterate over the rows in the CSV file
                    num_rows = 0
                    column_names = []
                    for line in f:

                        if num_rows == 0:
                            column_names = [col.strip() for col in line.split(',')]
                            num_rows += 1
                            continue

                        # Parse the row and construct the row key
                        valid_year = line.split(',')[0]
                        district_code = line.split(',')[1]
                        neighborhood_code = line.split(',')[3]
                        row_key = f"{table_name}_{district_code}_{neighborhood_code}_{valid_year}"

                        # Construct a dictionary of column name to value
                        data_dict = {
                            'data:District_Name': line.split(',')[2].encode('utf-8'),
                            'data:Neighborhood_Name': line.split(',')[4].encode('utf-8'),
                            'data:Population': line.split(',')[5].encode('utf-8'),
                            'data:Index RFD Barcelona = 100': line.split(',')[6].encode('utf-8'),
                        }

                        # Insert the data into HBase
                        table.put(row_key.encode('utf-8'), data_dict)

                        # Update metadata
                        num_rows += 1

                    # Insert metadata into HBase
                    metadata_dict = {
                        'metadata:num_rows': str(num_rows - 1).encode('utf-8'),
                        'metadata:num_cols': str(len(column_names)).encode('utf-8'),
                        'metadata:column_names': (','.join(column_names)).encode('utf-8'),
                        'metadata:ingestion_date': (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                    ).encode('utf-8')
                    }
                    table.put(f"{table_name}_{valid_year}_metadata".encode('utf-8'), metadata_dict)
                    self.logger.info(f"Imported {num_rows} rows into table '{table_name}' from file: {filename}.")

    def load_veh_index_motoritzacio(self):
        # Create a table for the data source
        table_name = 'veh_index_motoritzacio'
        self.create_table(table_name)

        # Get a handle to the table
        table = self.connection.table(table_name)

        # Iterate over the CSV files in the temporal landing zone
        csv_dir = os.path.join(self.temporal_landing_dir, self.temporal_landing_csv, table_name).replace('\\', '/')
        filenames = self.hdfs_client.list(csv_dir)
        for filename in filenames:
            if filename.endswith('.csv'):
                with self.hdfs_client.read(os.path.join(csv_dir, filename).replace('\\', '/'), encoding='utf-8') as f:
                    # Iterate over the rows in the CSV file
                    num_rows = 0
                    column_names = []
                    for line in f:

                        if num_rows == 0:
                            column_names = [col.strip() for col in line.split(',')]
                            num_rows += 1
                            continue

                        # Parse the row and construct the row key
                        valid_year = line.split(',')[0]
                        district_code = line.split(',')[1]
                        neighborhood_code = line.split(',')[3]
                        row_key = f"{table_name}_{district_code}_{neighborhood_code}_{valid_year}"

                        # Construct a dictionary of column name to value
                        data_dict = {
                            'data:District_Name': line.split(',')[2].encode('utf-8'),
                            'data:Neighborhood_Name': line.split(',')[4].encode('utf-8'),
                            'data:Seccio_Censal': line.split(',')[5].encode('utf-8'),
                            'data:Tipus_Vehicle': line.split(',')[6].encode('utf-8'),
                            'data:Index_Motoritzacio': line.split(',')[7].encode('utf-8'),
                        }

                        # Insert the data into HBase
                        table.put(row_key.encode('utf-8'), data_dict)

                        # Update metadata
                        num_rows += 1

                    # Insert metadata into HBase
                    metadata_dict = {
                        'metadata:num_rows': str(num_rows - 1).encode('utf-8'),
                        'metadata:num_cols': str(len(column_names)).encode('utf-8'),
                        'metadata:column_names': (','.join(column_names)).encode('utf-8'),
                        'metadata:ingestion_date': (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                    ).encode('utf-8')
                    }
                    table.put(f"{table_name}_{valid_year}_metadata".encode('utf-8'), metadata_dict)
                    self.logger.info(f"Imported {num_rows} rows into table '{table_name}' from file: {filename}.")

    def load_lookup_tables(self):
        print(self.host)

    def load_idealista(self):
        print(self.host)
