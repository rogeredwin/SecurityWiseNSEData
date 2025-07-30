import os
import pandas as pd
from io import StringIO
import requests
from datetime import datetime
import pytz
import time
from concurrent.futures import ThreadPoolExecutor

# Print working directory and files
print("🔍 Current working directory:", os.getcwd())
print("📁 Files in directory:", os.listdir())

# Change to script directory
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print("✅ Changed to script directory:", script_dir)
except Exception as e:
    print("❌ Failed to change directory:", e)

ist = pytz.timezone("Asia/Kolkata")

securityWiseDataFolder = "SecurityWiseData"

if not os.path.exists(securityWiseDataFolder):
    print(f"❌ Folder '{securityWiseDataFolder}' does not exist.")
else:
    print(f"✅ Found folder '{securityWiseDataFolder}'")

def getDeliveryDataFromNSE(symbol):
    print(f"🔄 Fetching delivery data from NSE for {symbol}")
    ref_columns = ['SYMBOL', 'SERIES', 'DATE1', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
    delivery_data_df = pd.DataFrame(columns=ref_columns)
    for year in range(1996, datetime.now(ist).year + 1):
        start_date = f"01-01-{year}"
        end_date = f"31-12-{year}"
        sym = symbol.replace("&", "%26")
        url = (
            f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?from={start_date}&to={end_date}&symbol={sym}&type=deliverable&series=ALL&csv=true"
        )
        for attempt in range(10):
            try:
                print(f"🌐 Attempting request for {symbol} year {year}, try {attempt+1}")
                response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                print(f"📡 Status code: {response.status_code}")
                if "<!DOCTYPE html>" in response.text[:100]:
                    print(f"🛑 NSE blocked or returned HTML for {symbol} {start_date}-{end_date}")
                    continue
                if response.status_code == 200 and response.content:
                    try:
                        df1 = pd.read_csv(StringIO(response.text))
                        df1.columns = ref_columns
                        if delivery_data_df.empty:
                            delivery_data_df = df1
                        elif not df1.empty:
                            delivery_data_df = pd.concat([delivery_data_df, df1], ignore_index=True)
                        break
                    except Exception as parse_error:
                        print(f"⚠️ Failed to parse CSV: {symbol}_{start_date}_{end_date} | Error: {parse_error}")
                else:
                    print(f"❌ HTTP Error: {symbol}_{start_date}_{end_date} | Status: {response.status_code}")
            except Exception as e:
                print(f"💥 Request Exception: {symbol}_{start_date}_{end_date} | Error: {e}")
            time.sleep(2)
    if not delivery_data_df.empty:
        delivery_data_df['SYMBOL'] = delivery_data_df['SYMBOL'].astype(str).str.strip()
        delivery_data_df['SERIES'] = delivery_data_df['SERIES'].astype(str).str.strip()
        delivery_data_df['DATE1'] = delivery_data_df['DATE1'].astype(str).str.strip()
        delivery_data_df['DATE1'] = pd.to_datetime(
            delivery_data_df['DATE1'].astype(str).str.strip(),
            format="%d-%b-%Y", errors='coerce'
        )
        delivery_data_df.dropna(subset=['DATE1'], inplace=True)
        delivery_data_df['DATE1'] = delivery_data_df['DATE1'].dt.strftime("%d-%m-%Y")
    print(f"✅ Completed fetching delivery data for {symbol}")
    return delivery_data_df

def process_symbol(symbolFile):
    print(f"🟢 Processing: {symbolFile}")
    symbol_path = os.path.join(securityWiseDataFolder, symbolFile)
    try:
        symbolFile_df = pd.read_csv(symbol_path)
        print(f"📄 Loaded file: {symbol_path} | Rows: {len(symbolFile_df)}")
    except Exception as e:
        print(f"❌ Failed to read {symbol_path}: {e}")
        return

    symbolFile_df = symbolFile_df[
        symbolFile_df['SERIES'].isin(["EQ", "BL", "SM"])
    ]
    symbolFile_df = symbolFile_df[
        (symbolFile_df['DELIV_QTY'] == "-") | (symbolFile_df['DELIV_PER'] == "-")
    ]
    if symbolFile_df.empty:
        print(f"ℹ️ No missing delivery data in {symbolFile}")
        return

    delivery_data_df = getDeliveryDataFromNSE(symbolFile[:-4])
    if delivery_data_df.empty:
        print(f"⚠️ No delivery data fetched for {symbolFile[:-4]}")
        return

    change = False
    for index, row in symbolFile_df.iterrows():
        filtered_df = delivery_data_df[
            (delivery_data_df['SERIES'] == row['SERIES']) &
            (delivery_data_df['DATE1'] == row['DATE1'])
        ]
        if not filtered_df.empty:
            change = True
            symbolFile_df.at[index, 'DELIV_QTY'] = filtered_df['DELIV_QTY'].values[0]
            symbolFile_df.at[index, 'DELIV_PER'] = filtered_df['DELIV_PER'].values[0]

    if change:
        try:
            symbolFile_df.to_csv(symbol_path, index=False)
            print(f"💾 Updated: {symbol_path}")
        except Exception as e:
            print(f"❌ Failed to write updated CSV: {e}")

def main():
    try:
        files = [f for f in os.listdir(securityWiseDataFolder) if f.endswith('.csv')]
        print(f"📦 CSV files found: {files}")
    except Exception as e:
        print(f"❌ Failed to list files in {securityWiseDataFolder}: {e}")
        return

    if not files:
        print("⚠️ No .csv files found to process.")
        return

    max_workers = os.cpu_count()
    print(f"🚀 Starting ThreadPoolExecutor with {max_workers} workers")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_symbol, files)

if __name__ == "__main__":
    main()
