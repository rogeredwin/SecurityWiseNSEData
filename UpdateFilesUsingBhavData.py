import os
import pandas as pd
import requests
import pytz
from io import StringIO
import time
from datetime import datetime, timedelta

ist = pytz.timezone("Asia/Kolkata")
valid_series = {"SM", "BE", "BZ", "EQ", "ST", "SZ", "BL"}

# Change to script directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))
log_file = "log.txt"
securityDataFolder = "SecurityWiseData"

def log_failure(reason):
    with open(log_file, "a") as log:
        log.write(f"{reason}\n")

def download_securitywisedata(START_DATE, END_DATE, SYMBOL):
    sample_csvs = [f for f in os.listdir(securityDataFolder) if f.endswith(".csv")]
    first_csv = os.path.join(securityDataFolder, sample_csvs[0])
    column_ref = pd.read_csv(first_csv)
    sym = SYMBOL.replace("&", "%26")
    url = (
        f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?from={START_DATE}&to={END_DATE}&symbol={sym}&type=priceVolumeDeliverable&series=ALL&csv=true"
    )
    for attempt in range(10):
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200 and response.content:
                try:
                    df = pd.read_csv(StringIO(response.text))
                    df.columns = column_ref.columns
                    if len(df) > 0:
                        df['SYMBOL'] = df['SYMBOL'].astype(str).str.strip()
                        df['SERIES'] = df['SERIES'].astype(str).str.strip()
                        df['DATE1'] = df['DATE1'].astype(str).str.strip()
                        df['DATE1'] = pd.to_datetime(df['DATE1'], format="%d-%b-%Y").dt.strftime("%d-%m-%Y")
                    return df # success, exit the function
                except Exception as parse_error:
                    print(f"⚠️ Failed to parse CSV: {SYMBOL}_{START_DATE}_{END_DATE}")
                    log_failure(SYMBOL, START_DATE, END_DATE, f"CSV Parse Error: {parse_error}")
                    return None # no point retrying if the file is corrupt
            else:
                print(f"❌ HTTP Error ({attempt+1}/10): {SYMBOL}_{START_DATE}_{END_DATE} | Status: {response.status_code}")
        except Exception as e:
            print(f"💥 Request Exception ({attempt+1}/10): {SYMBOL}_{START_DATE}_{END_DATE} | {e}")

        time.sleep(2)  # brief pause before retry
    print(f"🚫 Failed to download after 10 attempts: {SYMBOL}_{START_DATE}_{END_DATE}")
    return None

# download bhav data from NSE
def download_bhavdata(date, save_dir="BhavData"):
    os.makedirs(save_dir, exist_ok=True)
    for attempt in range(10):
        try:
            # Convert to DDMMYYYY
            ddmmyyyy = datetime.strptime(date, "%Y-%m-%d").strftime("%d%m%Y")
            yyyymmdd = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
            url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
            filename = os.path.join(save_dir, f"{yyyymmdd}_NSE.csv")
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200 and response.content:
                with open(filename, "wb") as f:
                    f.write(response.content)
                    return
                # print(f"✅ Downloaded: {filename}")
            else:
                log_failure(f"❌ Failed: {yyyymmdd} | Status: {response.status_code}")
        except Exception as e:
            print(f"💥 Error for {yyyymmdd}: {e}")
        time.sleep(2)

def fetch_symbolchange_file():
    url = (
        f"https://nsearchives.nseindia.com/content/equities/symbolchange.csv"
    )
    filename = f"symbolchange.csv"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code == 200 and response.content:
            df = pd.read_csv(StringIO(response.text))
            df.to_csv(filename, index=False)           
    except Exception as e:
        log_failure(f"Failed to fetch symbolchange file from NSE, use local fallback: {e}")
    symbolchange_df = pd.read_csv(filename, header = None, names=["Name", "Old Symbol", "New Symbol", "Change Date"])
    return symbolchange_df

symbolchange_df = fetch_symbolchange_file()
symbolchange_df["Old Symbol"] = symbolchange_df["Old Symbol"].astype(str).str.strip()
symbolchange_df["New Symbol"] = symbolchange_df["New Symbol"].astype(str).str.strip()

def updateFiles(date):
    key_cols = ['SYMBOL', 'SERIES', 'DATE1']
    bhavDataFilePath = "BhavData" +  "//" + datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d") + "_NSE.csv"
    bhavfile_df = pd.read_csv(bhavDataFilePath)
    date = datetime.strptime(date, "%Y-%m-%d").strftime("%d-%b-%Y")
    bhavfile_df.columns = bhavfile_df.columns.str.strip()
    bhavfile_df['SYMBOL'] = bhavfile_df['SYMBOL'].astype(str).str.strip()
    bhavfile_df['SERIES'] = bhavfile_df['SERIES'].astype(str).str.strip()
    bhavfile_df['DATE1'] = bhavfile_df['DATE1'].astype(str).str.strip()
    bhavfile_df['DATE1'] = pd.to_datetime(bhavfile_df['DATE1'], format="%d-%b-%Y").dt.strftime("%d-%m-%Y")
    bhavfile_df = bhavfile_df[bhavfile_df['SERIES'].isin(valid_series)]
    for idx, row in bhavfile_df.iterrows():
        securityDataFile = os.path.join(securityDataFolder, row['SYMBOL'] + ".csv")
        if not os.path.exists(securityDataFile):
            oldSymbolName = symbolchange_df.loc[symbolchange_df["New Symbol"] == row['SYMBOL'], "Old Symbol"]
            if not oldSymbolName.empty:
                old_name = oldSymbolName.iloc[0]
                oldNameSecurityDataFile = os.path.join(securityDataFolder, old_name + ".csv")
                if os.path.exists(oldNameSecurityDataFile):
                    os.rename(oldNameSecurityDataFile, securityDataFile)
        if not os.path.exists(securityDataFile):
            sample_csvs = [f for f in os.listdir(securityDataFolder) if f.endswith(".csv")]
            first_csv = os.path.join(securityDataFolder, sample_csvs[0])
            columns = pd.read_csv(first_csv, nrows=0).columns
            new_df = pd.DataFrame(columns=columns)
            new_df.to_csv(securityDataFile, index=False)     
        symbol_df = pd.read_csv(securityDataFile)
        is_duplicate = (
            (symbol_df[key_cols] == row[key_cols].values).all(axis=1)
        ).any()
        if not is_duplicate:
            row_df = pd.DataFrame([row])
            if symbol_df.empty:
                sample_csvs = [f for f in os.listdir(securityDataFolder) if f.endswith(".csv")]
                first_csv = os.path.join(securityDataFolder, sample_csvs[0])
                columns = pd.read_csv(first_csv, nrows=0).columns
                tryFromNSE_df = pd.DataFrame(columns=columns)
                for year in range(1996, datetime.now(ist).year + 1):
                    start_date = f"01-01-{year}"
                    end_date = f"31-12-{year}"
                    df = download_securitywisedata(start_date, end_date, row['SYMBOL'])
                    if df is not None and not df.empty:
                        if tryFromNSE_df.empty:
                            tryFromNSE_df = df
                        else:
                            tryFromNSE_df = pd.concat([tryFromNSE_df, df], ignore_index=True)
                symbol_df = tryFromNSE_df
                if not symbol_df.empty:
                    symbol_df.to_csv(securityDataFile, index=False)
                    print(f"{securityDataFile}     updated")
                    continue
            if symbol_df.empty:
                symbol_df = row_df
            else:
                symbol_df = pd.concat([symbol_df, row_df], ignore_index=True)
            symbol_df.to_csv(securityDataFile, index=False)
            print(f"{securityDataFile}     updated")

updateFiles("2025-07-28")
exit()

todayDate = datetime.now(ist).strftime("%Y-%m-%d")
yesterdayDate = (datetime.now(ist) - timedelta(days=1)).strftime("%Y-%m-%d")
beforeYesterdayDate = (datetime.now(ist) - timedelta(days=2)).strftime("%Y-%m-%d")

dates = {beforeYesterdayDate, yesterdayDate, todayDate}

for date in dates:
    bhavDataFilePath = "BhavData" +  "//" + datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d") + "_NSE.csv"
    if os.path.exists(bhavDataFilePath):
       continue
    else:
        download_bhavdata(date)
        if os.path.exists(bhavDataFilePath):
            updateFiles(date)