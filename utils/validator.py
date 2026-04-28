def validate_instrument_data(data):

    required_keys = ["Schema", "Operators", "Sample", "Procedure", "Results", "StartTime"]
    for key in required_keys:
        if key not in data:
            return False, f"Missing Required Key: {key}"
        
    sample_name = data["Sample"]["Name"]
    if sample_name is None:
        return False, "Missing Sample ID"
    
    operator_name = data["Operators"][0]["Name"]
    if operator_name is None:
        return False, "Missing Operator"

    mass_value = data["Sample"]["Mass"]["Value"]
    mass_val_unit = data["Sample"]["Mass"]["Unit"]["Name"]
    if mass_value <= 0:
        return False, f"Invalid Mass: {mass_value} {mass_val_unit}" 
    if mass_val_unit == "g":
        return False, f"Invalid Mass (Unit): {mass_value} {mass_val_unit}"
    
    rows = data["Results"]["Rows"]
    if not rows or not isinstance(rows, list):
        return False, "File contains no result rows" # returns false if rows is an empty list, a NoneType, or something that isn't a list
    

    return True, "File passed validation"