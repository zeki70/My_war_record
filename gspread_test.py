import os
import gspread
from google.oauth2.service_account import Credentials

# Prefer reading sensitive values from environment variables or Streamlit Secrets.
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
SERVICE_ACCOUNT_FILE = os.environ.get('SERVICE_ACCOUNT_FILE', 'service_account.json')
# Do NOT hardcode spreadsheet IDs in source. Read from env as fallback placeholder.
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '<YOUR_SPREADSHEET_ID>')
WORKSHEET_NAME = os.environ.get('WORKSHEET_NAME', 'シート1')

try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open_by_key(SPREADSHEET_ID) # IDで開く
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    
    print(f"'{SPREADSHEET_ID}' の '{WORKSHEET_NAME}' にアクセス成功！")
    
    # 簡単な読み取りテスト (例: A1セルの値)
    cell_value = worksheet.acell('A1').value
    print(f"A1セルの値: {cell_value}")
    
    # 最初の数行を取得
    data = worksheet.get_values('A1:E5') # 例としてA1からE5の範囲
    print("最初の数行のデータ:")
    for row in data:
        print(row)

except Exception as e:
    print(f"エラーが発生しました: {e}")