import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

FOLDER = "SecurityWiseData"

def process_file(file):
    try:
        filepath = os.path.join(FOLDER, file)
        df = pd.read_csv(filepath)
        # Convert DATE1 column to datetime
        df['DATE1'] = pd.to_datetime(df['DATE1'], format='%d-%m-%Y')
        # Sort by DATE1
        df_sorted = df.sort_values(by='DATE1')
        df_sorted.to_csv(filepath, index=False)
        print(f"Processed: {file}")
        
    except Exception as e:
        print(f"Error processing {file}: {e}")

def main():
    files = [f for f in os.listdir(FOLDER) if f.endswith('.csv')]

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(process_file, files)

if __name__ == "__main__":
    main()
