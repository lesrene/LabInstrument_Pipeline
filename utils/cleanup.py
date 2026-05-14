import os

def clean_db():
    list_of_files = os.listdir('data')    
    if len(list_of_files) > 1000:
        oldest_file = min(list_of_files, key=os.path.getctime)
        os.remove(os.path.abspath(oldest_file))

        with open("data/db_record.txt", 'r') as f:
            lines = f.readlines()
        
        with open("data/db_record.txt", 'w') as f:
            f.writelines(lines[1:])
