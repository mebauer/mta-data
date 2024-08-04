import pandas as pd
import duckdb
from datetime import datetime
import time

export_history = 'export-history.txt'

limit = 3000000
rows = limit
offset = 0

api_base = "https://data.ny.gov/resource/uhf3-t34z.csv"
con = duckdb.connect("subway-trips.db")

while rows == limit:
    
    api_path = f"{api_base}?$limit={limit}&$offset={offset}"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with open(export_history, 'a') as file:
        file.write(f"{timestamp},{api_path}\n")
    
    try:
        df = pd.read_csv(api_path)
        rows = df.shape[0]
        
        if offset == 0:
            con.sql("CREATE TABLE trips AS SELECT * FROM df")
        else:
            con.sql("INSERT INTO trips SELECT * FROM df")
            
        offset += limit

        time.sleep(3)
        
    except Exception as e:
        with open(export_history, 'a') as file:
            file.write(f"Error reading data: {e}\n")
        break

con.close()        
