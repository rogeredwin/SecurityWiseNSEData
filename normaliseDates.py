import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

FOLDER = "SecurityWiseData"

def normalize_date(val):
    """Try to convert any date format to DD-MM-YYYY string."""
    if pd.isnull(val):
        return val
    if isinstance(val, pd.Timestamp):
        return val.strftime("%d-%m-%Y")
    if isinstance(val, str):
        # Try multiple known formats
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(val, fmt).strftime("%d-%m-%Y")
            except ValueError:
                continue
    return None  # if it completely fails

def process_file(file):
    try:
        filepath = os.path.join(FOLDER, file)
        df = pd.read_csv(filepath)

        if 'DATE1' not in df.columns:
            print(f"Skipping {file}: 'DATE1' column not found.")
            return

        df['DATE1'] = df['DATE1'].apply(normalize_date)

        # Save the file back
        df.to_csv(filepath, index=False)
        print(f"Processed: {file}")
    except Exception as e:
        print(f"Error processing {file}: {e}")

def main():
    files = [f for f in os.listdir(FOLDER) if f.endswith('.csv')]

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(process_file, files)

if __name__ == "__main__":
    main()
