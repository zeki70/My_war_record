import os
import json
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]


def get_credentials_and_sheet_info():
    """Obtain credentials and spreadsheet info from (in order):
    1) Streamlit secrets: st.secrets['gcp_service_account'] (recommended)
       - Optional: st.secrets['SPREADSHEET_ID'] or st.secrets['gcp_service_account']['SPREADSHEET_ID']
    2) Environment variable GCP_SERVICE_ACCOUNT_JSON (raw JSON string)
    3) SERVICE_ACCOUNT_FILE path or local service_account.json file

    Spreadsheet ID is resolved in this priority:
      a) st.secrets['SPREADSHEET_ID']
      b) st.secrets['gcp_service_account']['SPREADSHEET_ID']
      c) environment variable SPREADSHEET_ID

    Returns (creds, spreadsheet_id, worksheet_name)
    """
    creds = None
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    worksheet_name = os.environ.get('WORKSHEET_NAME', 'Sheet1')

    # 1) Streamlit secrets
    try:
        import streamlit as st
        if hasattr(st, 'secrets'):
            # prefer top-level SPREADSHEET_ID in secrets
            spreadsheet_id = spreadsheet_id or st.secrets.get('SPREADSHEET_ID')
            # some users store creds under gcp_service_account and may include SPREADSHEET_ID
            if 'gcp_service_account' in st.secrets:
                creds_info = dict(st.secrets['gcp_service_account'])
                try:
                    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
                except Exception:
                    creds = None
                spreadsheet_id = spreadsheet_id or creds_info.get('SPREADSHEET_ID')
            worksheet_name = st.secrets.get('WORKSHEET_NAME', worksheet_name)
            if creds is not None:
                return creds, spreadsheet_id, worksheet_name
    except Exception:
        pass

    # 2) Raw JSON in env
    gcp_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    if gcp_json:
        try:
            creds_info = json.loads(gcp_json)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
            spreadsheet_id = spreadsheet_id or os.environ.get('SPREADSHEET_ID')
            return creds, spreadsheet_id, worksheet_name
        except Exception as e:
            print(f"Failed to parse GCP_SERVICE_ACCOUNT_JSON: {e}")

    # 3) File
    sa_file = os.environ.get('SERVICE_ACCOUNT_FILE', 'service_account.json')
    if os.path.exists(sa_file):
        try:
            creds = Credentials.from_service_account_file(sa_file, scopes=SCOPES)
            spreadsheet_id = spreadsheet_id or os.environ.get('SPREADSHEET_ID')
            return creds, spreadsheet_id, worksheet_name
        except Exception as e:
            print(f"Failed to load service account file {sa_file}: {e}")

    return None, spreadsheet_id, worksheet_name


def main():
    creds, spreadsheet_id, worksheet_name = get_credentials_and_sheet_info()
    if creds is None:
        print("No valid Google service account credentials found.\nFix: set Streamlit secrets 'gcp_service_account' (recommended), or set env GCP_SERVICE_ACCOUNT_JSON, or place a service_account.json file.")
        return
    if not spreadsheet_id:
        print("SPREADSHEET_ID not found. Set SPREADSHEET_ID in Streamlit secrets (recommended) or as env var.")
        return

    try:
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        print(f"Connected to spreadsheet '{spreadsheet_id}', worksheet '{worksheet_name}'.")
        cell_value = worksheet.acell('A1').value
        print(f"A1: {cell_value}")

        data = worksheet.get_values('A1:E5')
        print("Rows A1:E5:")
        for row in data:
            print(row)

    except Exception as e:
        print(f"Error accessing Google Sheets: {e}")


if __name__ == '__main__':
    main()