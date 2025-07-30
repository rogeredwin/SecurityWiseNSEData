import os
import pandas as pd
from io import StringIO
import requests
from datetime import datetime
import pytz
import time

# Change to script directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

ist = pytz.timezone("Asia/Kolkata")

securityWiseDataFolder = "SecurityWiseData"
    
def getDeliveryDataFromNSE(symbol):
    ref_columns = ['SYMBOL', 'SERIES', 'DATE1', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
    delivery_data_df = pd.DataFrame(columns=ref_columns)
    for year in range(1996, datetime.now(ist).year + 1):
        start_date = f"01-01-{year}"
        end_date = f"31-12-{year}"
        sym = symbol.replace("&", "%26")
        url = (
            f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?from={start_date}&to={end_date}&symbol={sym}&type=deliverable&series=ALL&csv=true"
        )
        for attempt in range(5):
            try:
                response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout = 10)
                if response.status_code == 200 and response.content:
                    try:
                        df1 = pd.read_csv(StringIO(response.text))
                        df1.columns = ref_columns
                        if delivery_data_df.empty:
                            delivery_data_df = df1
                        else:
                            if not df1.empty:
                                delivery_data_df = pd.concat([delivery_data_df, df1], ignore_index=True)
                        break
                    except Exception as parse_error:
                        print(f"⚠️ Failed to parse CSV: {symbol}_{start_date}_{end_date}")
                else:
                    print(f"❌ HTTP Error: {symbol}_{start_date}_{end_date} | Status: {response.status_code}")
            except Exception as e:
                print(f"💥 Request Exception: {symbol}_{start_date}_{end_date}")
            time.sleep(2)  # brief pause before retry
    if len(delivery_data_df) > 0:
        delivery_data_df['SYMBOL'] = delivery_data_df['SYMBOL'].astype(str).str.strip()
        delivery_data_df['SERIES'] = delivery_data_df['SERIES'].astype(str).str.strip()
        delivery_data_df['DATE1'] = delivery_data_df['DATE1'].astype(str).str.strip()
        delivery_data_df['DATE1'] = pd.to_datetime(
            delivery_data_df['DATE1'].astype(str).str.strip(),
            format="%d-%b-%Y", errors='coerce'
        )
        delivery_data_df.dropna(subset=['DATE1'], inplace=True)
        delivery_data_df['DATE1'] = delivery_data_df['DATE1'].dt.strftime("%d-%m-%Y")
    return delivery_data_df

def process_symbol(symbolFile):
    print(f"Processing: {symbolFile}")
    symbolFile = os.path.join(securityWiseDataFolder, symbolFile)
    symbolFile_df = pd.read_csv(symbolFile)
    symbolFile_df = symbolFile_df[(symbolFile_df['SERIES'] == "EQ") | (symbolFile_df['SERIES'] == "BL") | (symbolFile_df['SERIES'] == "SM")]
    symbolFile_df = symbolFile_df[(symbolFile_df['DELIV_QTY'] == "-") | (symbolFile_df['DELIV_PER'] == "-")]
    if len(symbolFile_df) == 0:
        return
    delivery_data_df = getDeliveryDataFromNSE(symbolFile[:-4])
    if delivery_data_df.empty:
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
        symbolFile_df.to_csv(symbolFile, index=False)
        print(f"{symbolFile_df} updated")

def main():
    files = os.listdir(securityWiseDataFolder)
    print(files)
    for file in files:
        process_symbol(file)

if __name__ == "__main__":
    main()
