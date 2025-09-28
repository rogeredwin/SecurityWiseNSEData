import os
import pandas as pd

# Folder containing your CSV files
FOLDER_PATH = "SecurityWiseData"   # change this if needed

def clean_csv_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            try:
                # Read CSV
                df = pd.read_csv(file_path)

                # Drop duplicate rows
                df = df.drop_duplicates()

                # Save back (overwrite same file)
                df.to_csv(file_path, index=False)

                print(f"Cleaned: {filename} (removed duplicates)")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    clean_csv_files(FOLDER_PATH)
