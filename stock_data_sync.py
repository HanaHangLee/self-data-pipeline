import os
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from preprocess import load_and_resample_timeseries

# Load environment variables from .env
load_dotenv()

KAGGLE_DATASET = os.getenv("KAGGLE_DATASET")
LOCAL_CSV_FILE = os.getenv("LOCAL_CSV_FILE")
SHEET_NAME = os.getenv("SHEET_NAME")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")

# Step 1: Download data from Kaggle
def download_data_from_kaggle():
    os.environ['KAGGLE_CONFIG_DIR'] = os.path.abspath('.utils')

    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(KAGGLE_DATASET, path='.utils', unzip=True)
    print("✅ Kaggle dataset downloaded successfully.")

# Step 2: Load and preprocess CSV data
def load_csv_to_dataframe():
    df_resampled = load_and_resample_timeseries(os.path.join(".utils", LOCAL_CSV_FILE))
    print(f"✅ Loaded {len(df_resampled)} rows from CSV.")
    return df_resampled

# Step 3: Upload data to Google Sheets
def upload_to_google_sheets(df):
    creds = Credentials.from_service_account_file(
        os.path.join(".utils", GOOGLE_CREDENTIALS_PATH),
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = sheet.worksheet(SHEET_NAME)
        sheet.del_worksheet(worksheet)
        print("🔁 Old worksheet deleted.")
    except gspread.exceptions.WorksheetNotFound:
        pass

    worksheet = sheet.add_worksheet(title=SHEET_NAME, rows="1000", cols="20")
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("✅ Data uploaded to Google Sheets.")

def main():
    download_data_from_kaggle()
    df = load_csv_to_dataframe()
    upload_to_google_sheets(df)

if __name__ == "__main__":
    main()
