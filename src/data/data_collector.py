import os
import csv
import json
import glob
import logging
from hdfs import InsecureClient


class DataCollector:
    """
        A class for collecting data from local directories and uploading it to HDFS.

        Args:
            global_data_dir (str): Path to the directory containing the data sources files to upload.
            hdfs_host (str): Hostname or IP address of the HDFS Namenode.
            hdfs_port (str): Port number of the HDFS Namenode.
            hdfs_user (str): Username to use when connecting to HDFS.
            logger (logging.Logger): Logger object for logging messages.

        Attributes:
            global_data_dir (str): Path to the directory containing the data sources files to upload.
            hdfs_host (str): Hostname or IP address of the HDFS Namenode.
            hdfs_port (str): Port number of the HDFS Namenode.
            hdfs_user (str): Username to use when connecting to HDFS.
            logger (logging.Logger): Logger object for logging messages.
            client (hdfs.InsecureClient): HDFS client object for interacting with HDFS.

        Methods:
            upload_csv_files_to_hdfs(hdfs_dir):
                Uploads all CSV files in the local CSV directory to the specified HDFS directory.
            upload_json_files_to_hdfs(hdfs_dir):
                Uploads all JSON files in the local JSON directory to the specified HDFS directory.
        """

    def __init__(self, global_data_dir, hdfs_host, hdfs_port, hdfs_user, logger):
        """
            Initializes a new instance of the DataCollector class.

            Args:
                global_data_dir (str): Path to the directory containing the data sources files to upload.
                hdfs_host (str): Hostname or IP address of the HDFS Namenode.
                hdfs_port (str): Port number of the HDFS Namenode.
                hdfs_user (str): Username to use when connecting to HDFS.
                logger (logging.Logger): Logger object for logging messages.
        """
        self.global_data_dir = global_data_dir
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_user = hdfs_user
        self.logger = logger
        self.client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}', user=self.hdfs_user)
        self.logger.info(f"Connection to HDFS has been established successfully.")


    def upload_csv_files_to_hdfs(self, hdfs_dir):
        """
            Uploads all CSV files in the local CSV directory to the specified HDFS directory.

            Args:
                hdfs_dir (str): The HDFS directory to upload the CSV files to.
        """
        # Get a list of all CSV files in the directory
        csv_files = glob.glob(os.path.join(self.global_data_dir, '**/*.csv'), recursive=True)

        # Check if directory exists
        if self.client.status(hdfs_dir, strict=False) is None:
            # Create directory
            self.client.makedirs(hdfs_dir)
            self.logger.info(f"Directory {hdfs_dir} created successfully.")
        else:
            self.logger.info(f"Directory {hdfs_dir} already exists.")

        # Loop through CSV files in local directory
        for filepath in csv_files:
            # Load CSV file
            with open(filepath, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                # Convert CSV data to bytes
                data_bytes = bytes('\n'.join([','.join(row) for row in csv_reader]), encoding='utf-8')

            # Upload CSV file to HDFS directory
            hdfs_path = os.path.join(hdfs_dir, os.path.basename(filepath))
            with self.client.write(hdfs_path, overwrite=True) as writer:
                writer.write(data_bytes)

            self.logger.info(f"File {filepath} uploaded to {hdfs_path} successfully.")

    def upload_json_files_to_hdfs(self, hdfs_dir):
        """
            Uploads all JSON files in the local JSON directory to the specified HDFS directory.

            Args:
                hdfs_dir (str): The HDFS directory to upload the JSON files to.
        """

        # Get a list of all JSON files in the directory
        json_files = glob.glob(os.path.join(self.global_data_dir, '**/*.json'), recursive=True)

        # Check if directory exists
        if self.client.status(hdfs_dir, strict=False) is None:
            # Create directory
            self.client.makedirs(hdfs_dir)
            self.logger.info(f"Directory {hdfs_dir} created successfully.")
        else:
            self.logger.info(f"Directory {hdfs_dir} already exists.")

        # Loop through JSON files in local directory
        for filepath in json_files:
            # Load JSON file
            with open(filepath, 'r') as f:
                data = json.load(f)

            # Convert JSON data to bytes
            data_bytes = json.dumps(data).encode('utf-8')

            # Upload JSON file to HDFS directory
            hdfs_path = os.path.join(hdfs_dir, os.path.basename(filepath))
            with self.client.write(hdfs_path, overwrite=True) as writer:
                writer.write(data_bytes)

            self.logger.info(f"File {filepath} uploaded to {hdfs_path} successfully.")