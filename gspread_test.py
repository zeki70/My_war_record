import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
SERVICE_ACCOUNT_FILE = 'service_account.json' # 実際のキーファイル名
SPREADSHEET_ID = "1V9guZQbpV8UDU_W2pC1WBsE1hOHqIO4yTsG8oGzaPQU"
WORKSHEET_NAME = "シート1" # ★★★ Waic-戦績のシート名 ★★★

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