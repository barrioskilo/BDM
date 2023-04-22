import datetime
import json
import happybase
import logging
import os
import re


class PersistenceLoader:
    def __init__(self, host, port, logger):
        self.host = host
        self.port = port
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None

    def connect(self):
        self.logger.info(f"Connecting to HBase at {self.host}:{self.port}")
        try:
            con = happybase.Connection(self.host, self.port)
            con.open()
            self.connection = con
            self.logger.info("Successfully connected to HBase")
        except Exception as e:
            self.logger.error(f"Error connecting to HBase: {e}")

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

    def load_opendatabcn_income(self):
        print(self.host)

    def load_idealista(self):
        print(self.host)

    def load_veh_index_motoritzacio(self):
        print(self.host)

    def load_lookup_tables(self):
        print(self.host)
