import sqlite3
import pandas as pd

DB_PATH = "lab_data.db"

def get_connection(db_path=DB_PATH):
    # connection to the SQLite database
    return sqlite3.connect(db_path)


def create_db_schema(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Experiments (
        experiment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sample_name TEXT,
        sample_mass_value REAL,
        operator_name TEXT,
        start_time TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Measurements (
        measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
        experiment_id INTEGER,
        time_min REAL,
        temp_c REAL,
        heat_flow_mw REAL,
        procedure_step_id TEXT,
        FOREIGN KEY (experiment_id) REFERENCES Experiments (experiment_id)           
    );

    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Quarantine_Logs (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        message TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    """)

    conn.commit()
    conn.close()

def search_experiments_sql(search_term):
    # searches for experiments matching user inputted name or ID directly in the DB
    conn = get_connection()
    query = """
        SELECT experiment_id, sample_name, start_time 
        FROM Experiments 
        WHERE sample_name LIKE ? OR experiment_id LIKE ?
        ORDER BY experiment_id DESC
    """
    df = pd.read_sql(query, conn, params=[f"%{search_term}%", f"%{search_term}%"])
    conn.close()
    return df

def get_measurements_by_experiment(exp_id):
    # gets all rows for a specific experiment to plot them
    conn = get_connection()
    query = "SELECT * FROM Measurements WHERE experiment_id = ?"
    df = pd.read_sql(query, conn, params=[exp_id])
    conn.close()
    return df

def get_quarantine_logs():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Quarantine_Logs ORDER BY timestamp DESC", conn)
    conn.close()
    return df

def get_filtered_experiments(name_query=None, operator=None, start_date=None, end_date=None):
    conn = get_connection()
    
    # query regardless
    query = "SELECT experiment_id, sample_name, operator_name, start_time FROM Experiments WHERE 1=1" # where statement always true to accomodate added filters later
    params = []

    # adds filters based on what the user actually puts in 
    if name_query:
        query += " AND sample_name LIKE ?"
        params.append(f"%{name_query}%")
    
    if operator:
        query += " AND operator_name = ?"
        params.append(operator)
        
    if start_date and end_date:
        query += " AND start_time BETWEEN ? AND ?"
        params.append(f"{start_date} 00:00:00")
        params.append(f"{end_date} 23:59:59")

    query += " ORDER BY experiment_id DESC"
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df