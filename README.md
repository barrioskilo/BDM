# Big Data Management - Universitat Politècnica de Catalunya
## Implementation of a (Big) Data Management Backbone
### Barcelona Rentals and Territorial Income Distribution
***
### Instructions for Executing the Code

- Download Python version 3.10
- Clone this repository locally
  - ``` git clone git@github.com:barrioskilo/BDM.git```
- Create a virtual environment with Python 3.10 interpreter and run the following command to install the required libraries:
  - ```
      pip install requirements.txt
    ```
- Create a folder `data_sources` inside BDM folder.
  - Create a folder per data source inside `data_sources` folder. E.g.:
    - `./data_sources/idealista`
    - `./data_sources/opendatabcn-income`
    - `./data_sources/lookup_tables`
  - Insert the raw data source files inside the appropriate folder.
- Add a `.env` file inside BDM folder, including the following parameters with their appropriate values:
  - ```
    HDFS_HOST='...' # Change This
    HDFS_PORT=9870
    HDFS_USER='...' # Change This
    GLOBAL_DATA_DIR_PATH='./data_sources'
    TEMPORAL_LANDING_DIR_PATH = '/temporal_landing'
    TEMPORAL_LANDING_CSV_DIR_PATH= 'temporal_landing_CSV'
    TEMPORAL_LANDING_JSON_DIR_PATH= 'temporal_landing_JSON'
    OPEN_DATA_API_KEY= '...' # Change This
    ```
- Execute `main.py`

### Limitations
There is a problem with `macOS` systems (mainly with the paths), and it needs to be investigated.
Execution in `Windows` machines is completed smoothly.