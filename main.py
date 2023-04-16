import os
from dotenv import load_dotenv
import logging
import logging.handlers
from src.data.data_collector import DataCollector


# Create logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler which logs debug messages
log_file = os.path.join('logs', 'main.log')
log_dir = os.path.dirname(log_file)

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5)
file_handler.setLevel(logging.DEBUG)

# Create console handler which logs info messages
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)



# Load environment variables from config..env
load_dotenv()

# Define local data directory path from environment variables
GLOBAL_DATA_DIR = os.getenv('GLOBAL_DATA_DIR')

# Define HDFS directory paths from temporal landing zone

TEMPORAL_LANDING_CSV_DIR_PATH = os.getenv('TEMPORAL_LANDING_CSV_DIR_PATH')
TEMPORAL_LANDING_JSON_DIR_PATH = os.getenv('TEMPORAL_LANDING_JSON_DIR_PATH')

# Define HDFS connection parameters from environment variables
HDFS_HOST = os.getenv('HDFS_HOST')
HDFS_PORT = os.getenv('HDFS_PORT')
HDFS_USER = os.getenv('HDFS_USER')


def main():

    try:
        # Initialize a DataCollector instance
        data_collector = DataCollector(GLOBAL_DATA_DIR, HDFS_HOST, HDFS_PORT, HDFS_USER, logger)

        # Run the data collection functions
        data_collector.upload_csv_files_to_hdfs(TEMPORAL_LANDING_CSV_DIR_PATH)
        data_collector.upload_json_files_to_hdfs(TEMPORAL_LANDING_JSON_DIR_PATH)

        logger.info('Data collection completed successfully.')

    except Exception as e:

        logger.exception(f'Error occurred during data collection: {e}')


if __name__ == '__main__':
    main()
