import json
import time
import random
import os
from cleanup import clean_db

def generate_realistic_data(total_rows):
    rows = []
    rows_per_step = total_rows // 4
    current_time = 0.00
    current_temp = 0.00
    for i in range(total_rows+1):
        if i < total_rows - (total_rows - rows_per_step):
            step_id = "STEP-001"
            current_temp = 0.00 + random.uniform(-0.05, 0.05)
        elif i < total_rows - (total_rows - (2*rows_per_step)):
            step_id = "STEP-002"
            current_temp += 35
        elif i < total_rows - (total_rows - (3*rows_per_step)):
            step_id = "STEP-003"
            current_temp = current_temp + random.uniform(-0.05, 0.05)
        else:
            step_id = "STEP-004"
            current_temp -= 35.00

        current_time += 0.1

        row = {
                "Time_min": current_time,
                "Temp_C": current_temp,
                "Heat_Flow_mW": random.uniform(0.50, 1.50),
                "Procedure_Step_Id": step_id
            }
        
        rows.append(row)

    return rows

def generate_lab_json(data_points = 20):
    operator = random.choice(['AE', 'GG', 'IN', 'CD', 'AL', 'GWC', 'KJ', 'MMD'])
    sample_id = int(time.time()) if random.random() > 0.005 else None # simulating data quality issue where sample id isn't found
    sample_name = f"ID-{sample_id}_PLA_Polymer" 

    data = {
        "Schema": {"URL":"https://software.tainstruments.com/schemas/TRIOSJSONExportSchema"},
        "Operators": [{"Name":operator}],
        "Sample": {
            "Name": sample_name,
            "Mass": {
            "Value": random.uniform(15.0,25.0) if random.random() > 0.05 else -1.0, # normal val except sometimes when random.random() is below .05 val is -1 to simulate a data quality issue
            "Unit": {
                "Name": "mg" if random.random() > 0.005 else "g" # also simulating data quality issue
            }
            },
            "PanType": "Tzero Aluminum"
        },
        "Procedure": {
            "Name": "Custom",
            "Steps": [
            {"Name": "Equilibrate 0.00 °C", "Id": "STEP-001" },
            {"Name": "Ramp 35.0 °C/min to 210.00 °C", "Id": "STEP-002" },
            {"Name": "Isothermal 2.0 min", "Id": "STEP-003" },
            {"Name": "Ramp 35.0 °C/min to 0.00 °C", "Id": "STEP-004" }
            ]
        },
        "Results": {
            "ColumnHeaders":{
                "Procedure_Step_Id": {"DisplayName": "Procedure Step Id"},
                "Time_min": {"DisplayName": "Time", "Unit": {"Name": "min"}}, 
                "Temp_C": {"DisplayName": "Temperature", "Unit": {"Name": "°C"}},
                "HeatFlow_mW": {"DisplayName": "Heat Flow", "Unit": {"Name": "mW"}}
            },
            "Rows": generate_realistic_data(data_points)
        },
        "StartTime": time.asctime()   
    }

    return data, sample_id

def save_json(data, identification):
    os.makedirs("data/raw", exist_ok=True)
    if identification is None:
        identification = "NULL"

    filename = f"data/raw/instrument_run_{identification}.json"   
    with open(filename, 'w') as f:
        json.dump(data, f)

    with open("data/db_record.txt", 'a') as f1: # for internal record keeping so I can know which sample ids are currently valid
        f1.write(f"instrument_run_{identification}")
    print(f"Generated: {filename}")


if __name__ == "__main__":
    print("Starting Lab Instrument Simulator (Ctrl+C to stop)...")
    experiment, s_id = generate_lab_json()
    while True:
        save_json(experiment, s_id)
        clean_db()
        time.sleep(10)
