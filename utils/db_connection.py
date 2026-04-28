import sqlite3

DB_PATH = "lab_data.db"

def get_connection(DB_PATH):
    """Returns a connection object to the SQLite database."""
    return sqlite3.connect(DB_PATH)


def create_db_schema(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
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
        measurement_id INTEGER PRIMAARY KEY AUTOINCREMENT,
        experiment_id INTEGER,
        time_min REAL,
        temp_c REAL,
        heat_flow_mw REAL,
        procedure_step_id TEXT,
        FOREIGN KEY (experiment_id) REFERENCES Experiments (experiment_id)           
    );

    """)

    conn.commit()
    conn.close()