import os
import json
import logging
import pandas as pd
from utils.validator import validate_instrument_data
import sqlite3
from utils.db_connection import create_db_schema, get_connection
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import time

# sets up a log to keep info about the ingestion & processing of the files
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Pipeline")

class LabDataHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.src_path.endswith(".json"): # files that aren't jsons get ignored
            return
        
        logger.info(f"New file entered pipeline: {event.src_path}")
        self.execute_etl(event.src_path)

    def quarantine_file(self, file_path):
        quarantine_folder = "data/quarantine"
        os.makedirs(quarantine_folder, exist_ok=True)
        file_name = os.path.basename(file_path)
        new_path = os.path.join(quarantine_folder, file_name)
        os.rename(file_path, new_path)
        logger.info("Moved: {file_name} -> {quarantine_folder}")

    def process_file(self, file_path):
        processed_folder = "data/processed"
        os.makedirs(processed_folder, exist_ok=True)
        file_name = os.path.basename(file_path)
        new_path = os.path.join(processed_folder, file_name)
        os.rename(file_path, new_path)
        logger.info(f"Moved: {file_name} -> {processed_folder}")

    def execute_etl(self, file_path):
        # step 1: read in json
        with open(file_path, 'r') as f:
            data = json.load(f)

        # step 2: determine if we need to quarantine, if no continue, if yes redirect
        is_valid, message = validate_instrument_data(data)
        # step 2 redirect: move to data/quarantine, log move to qurantine bucket with info about data quality issue
        if not is_valid:
            logger.error(f"VALIDATION_FAILED: {message} in {file_path}")
            self.quarantine_file(file_path)
            return
        # step 3: any pre-processing and flatten the json
        # step 4: add rows to sqlite database, log addition of rows to database
        try:
            metadata = {
                'sample_name': data['Sample']['Name'],
                'sample_mass_value': data['Sample']['Mass']['Value'],
                'operator_name': data['Operators'][0]['Name'],
                'start_time': data['StartTime']
            }

            create_db_schema()
            conn = get_connection()

            with conn:

                cursor = conn.cursor()
                cursor.execute("""INSERT INTO Experiments (sample_name, sample_mass_value, operator_name, start_time) 
                            VALUES (?, ?, ?, ?)""", 
                            (metadata['sample_name'], metadata['sample_mass_value'], metadata['operator_name'], metadata['start_time']))
                exp_id = cursor.lastrowid
                df = pd.DataFrame(data['Results']['Rows'])
                df['experiment_id'] = exp_id
                df.to_sql('Measurements', conn, if_exists='append', index=False)

            conn.close()
            logger.info(f"Successfully ingested and added rows to database from: {file_path}")

            # step 5: move to data/processed, log move to processed bucket
            logger.info(f"VALIDATION_PASSED: {message} in {file_path}")
            self.process_file(file_path)
        
        except Exception as e:
            logger.error(f"Database Error for {file_path}: {e}")


if __name__ == "__main__":
    path_to_watch = "data/raw" 

    event_handler = LabDataHandler()
    
    observer = Observer()

    observer.schedule(event_handler, path_to_watch, recursive=False)
    
    logger.info(f"Pipeline started. Watching directory: {path_to_watch}")
    observer.start()
    
    try:
        while True:
            time.sleep(1) # keeps the script alive so it doesn't just finish
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Pipeline stopped by user.")
    
    observer.join()