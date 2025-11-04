import streamlit as st
import pandas as pd
from datetime import datetime
import io # CSVエクスポートに io が直接使われていないが、将来的な用途や互換性のため残置可
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from streamlit.errors import StreamlitAPIException
from streamlit_cookies_manager import EncryptedCookieManager # ★インポート

st.set_page_config(layout="wide")

# --- 定数定義 ---
SPREADSHEET_NAME_DISPLAY = "Shadowverse戦績管理" # 変更
SPREADSHEET_ID = st.secrets["gcp_service_account"]["SPREADSHEET_ID"]
WORKSHEET_NAME = "シート1"
COLUMNS = [ # 'format' を追加
    'season', 'timestamp', 'environment', 'format', 'group', 'my_deck', 'my_deck_type','my_class', 
    'opponent_deck', 'opponent_deck_type','opponent_class',   'first_second',
    'result', 'finish_turn', 'memo'
]
NEW_ENTRY_LABEL = "（新しい値を入力）"
SELECT_PLACEHOLDER = "--- 選択してください ---" # 分析用
ALL_TYPES_PLACEHOLDER = "全タイプ" # 分析用

# --- パスワード認証のための設定 ---
def get_app_password():
    # Fail closed: require password to be set in Streamlit Secrets
    if hasattr(st, 'secrets') and "app_credentials" in st.secrets and "password" in st.secrets["app_credentials"]:
        return st.secrets["app_credentials"]["password"]
    else:
        st.error("アプリケーションパスワードがStreamlit Secretsに設定されていません。デプロイ前に st.secrets['app_credentials']['password'] を設定してください。")
        st.stop()
CORRECT_PASSWORD = get_app_password()

# ★★★ ここにクッキーマネージャの初期化コードを配置 ★★★
# st.secrets から暗号化キーを取得 (事前にStreamlit CloudのSecretsに設定してください)
# 例: [app_credentials]
#      cookie_encryption_key = "あなたの生成した秘密のキー"
if hasattr(st, 'secrets') and "app_credentials" in st.secrets and "cookie_encryption_key" in st.secrets["app_credentials"]:
    cookie_encryption_key = st.secrets["app_credentials"]["cookie_encryption_key"]
else:
    st.error("クッキー暗号化キーがStreamlit Secretsに設定されていません。デプロイ前に st.secrets['app_credentials']['cookie_encryption_key'] を設定してください。")
    st.stop()

cookies = EncryptedCookieManager(
    password=cookie_encryption_key, # Secretsから取得したキーを使用
    # prefix="my_app_cookie_", # 必要に応じてプレフィックスを設定
    # path="/",                 # 必要に応じてパスを設定
)
if not cookies.ready():
    # cookies.ready() は、クッキーがブラウザからロードされるのを待つためのものです。
    # これが False を返す場合、クッキー操作の準備ができていない可能性があります。
    # st.stop() を呼ぶとアプリが停止するので、状況に応じてエラー表示やリトライを検討してもよいですが、
    # 通常はすぐに True になるはずです。
    st.error("クッキーマネージャの準備ができませんでした。ページを再読み込みしてみてください。")
    st.stop()
# ★★★ 初期化コードここまで ★★★

# --- Google Sheets 連携 ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
def get_gspread_client():
    creds = None
    use_streamlit_secrets = False
    if hasattr(st, 'secrets'):
        try:
            if "gcp_service_account" in st.secrets:
                use_streamlit_secrets = True
        except StreamlitAPIException:
            pass
    if use_streamlit_secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        try:
            creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
        except Exception as e:
            st.error(f"サービスアカウントの認証情報ファイル (service_account.json) の読み込みに失敗しました: {e}")
            return None
    try:
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Google Sheetsへの接続に失敗しました: {e}")
        return None

# --- データ操作関数 ---
def load_data(spreadsheet_id, worksheet_name):
    client = get_gspread_client()
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを読み込めません。認証情報を確認してください。")
        empty_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col == 'timestamp': empty_df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': empty_df[col] = pd.Series(dtype='Int64')
            else: empty_df[col] = pd.Series(dtype='object')
        return empty_df
        
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # ヘッダーチェックと更新ロジック
        header_updated = False
        if worksheet.row_count == 0: # シートが完全に空の場合
            worksheet.update('A1', [COLUMNS], value_input_option='USER_ENTERED')
            st.info("スプレッドシートにヘッダー行を書き込みました。")
            header_updated = True
        else:
            current_headers = worksheet.row_values(1) # 最初の行を取得
            # ヘッダーがCOLUMNSと完全に一致しない場合に更新
            if not current_headers or list(current_headers) != COLUMNS:
                worksheet.update('A1', [COLUMNS], value_input_option='USER_ENTERED')
                if not current_headers:
                     st.info("スプレッドシートにヘッダー行を書き込みました。")
                else:
                     st.warning("スプレッドシートのヘッダーを期待される形式に更新しました。")
                header_updated = True
        
        # ヘッダーが更新された可能性も考慮し、データを読み込む
        # header=0 は get_as_dataframe のデフォルトだが、明示的に指定
        df = get_as_dataframe(worksheet, evaluate_formulas=False, header=0, na_filter=True)

        # Normalize common timestamp column names from external sources
        # Some exports/sheets use 'date' or 'datetime' as the header. Map them to 'timestamp'.
        if df is not None and 'timestamp' not in df.columns:
            for alt in ['date', 'datetime', 'time']:
                if alt in df.columns:
                    try:
                        df = df.rename(columns={alt: 'timestamp'})
                        break
                    except Exception:
                        pass

        # Heuristic: if still no 'timestamp' column, try to detect a column whose values look like datetimes
        if df is not None and 'timestamp' not in df.columns:
            detected_col = None
            for col in df.columns:
                try:
                    parsed = parse_timestamp_series(df[col])
                except Exception:
                    parsed = pd.Series([pd.NaT] * len(df))
                # consider this column a timestamp if we parsed at least one non-NaT with reasonable year
                non_null = parsed.dropna()
                if not non_null.empty:
                    years = non_null.dt.year
                    # require at least one year in plausible range
                    if ((years >= 1900) & (years <= 2100)).any():
                        detected_col = col
                        break
            if detected_col:
                try:
                    df = df.rename(columns={detected_col: 'timestamp'})
                    # show a short notice in Streamlit so it's visible during runtime
                    try:
                        st.info(f"Detected timestamp column: '{detected_col}' -> using as 'timestamp'.")
                    except Exception:
                        pass
                except Exception:
                    pass

        # COLUMNS に基づいて DataFrame を整形し、不足列は適切な型で追加
        # この処理は、get_as_dataframe がヘッダー行を正しく解釈した後に実行される
        temp_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col in df.columns:
                temp_df[col] = df[col]
            else: # dfに列が存在しない場合は、空のSeriesを適切な型で作成
                if col == 'timestamp': temp_df[col] = pd.Series(dtype='datetime64[ns]')
                elif col == 'finish_turn': temp_df[col] = pd.Series(dtype='Int64')
                else: temp_df[col] = pd.Series(dtype='object')
        df = temp_df
        
        # 型変換
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        if 'finish_turn' in df.columns:
            df['finish_turn'] = pd.to_numeric(df['finish_turn'], errors='coerce').astype('Int64')

        # 文字列として扱う列の処理 (my_class, opponent_class を含む)
        string_cols = ['my_deck_type', 'my_class', 'opponent_deck_type', 'opponent_class',
                       'my_deck', 'opponent_deck', 'season', 'memo',
                       'first_second', 'result', 'environment', 'format', 'group']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
            else: # 通常はこのケースは起こりにくいが念のため
                df[col] = pd.Series(dtype='str').fillna('')
        
        # 最終的にCOLUMNSの順序と列構成を保証
        df = df.reindex(columns=COLUMNS)

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"スプレッドシート (ID: {spreadsheet_id}) が見つからないか、アクセス権がありません。共有設定を確認してください。")
        df = pd.DataFrame(columns=COLUMNS) # 空のDataFrameを返す
        for col in COLUMNS: # 型情報を付与
            if col == 'timestamp': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"ワークシート '{worksheet_name}' がスプレッドシート (ID: {spreadsheet_id}) 内に見つかりません。")
        df = pd.DataFrame(columns=COLUMNS) # 空のDataFrameを返す
        for col in COLUMNS: # 型情報を付与
            if col == 'timestamp': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    except Exception as e:
        st.error(f"Google Sheetsからのデータ読み込み中に予期せぬエラーが発生しました: {type(e).__name__}: {e}")
        df = pd.DataFrame(columns=COLUMNS) # 空のDataFrameを返す
        for col in COLUMNS: # 型情報を付与
            if col == 'timestamp': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    return df


# --- 日付パースヘルパ ---
def parse_timestamp_series(series):
    """Try to robustly parse a Series of timestamps.
    Supports strings like 'YYYY/MM/DD HH:MM:SS' and other common formats.
    Returns a Series of pd.Timestamp (NaT when unparseable).
    """
    if series is None:
        return pd.Series([], dtype='datetime64[ns]')

    # If already datetime dtype, normalize and return
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series)

    # Work on a string copy and clean common invisible characters then strip
    s = series.astype(str).fillna('')
    s = s.str.replace('\u00A0', ' ', regex=False)
    s = s.str.replace('\u200B', '', regex=False)
    s = s.str.replace('\uFEFF', '', regex=False)
    s = s.str.strip()

    # Extract a date-like substring if the cell contains additional text
    # Pattern:  YYYY[-/]MM[-/]DD[ T]HH:MM[:SS]  (time part optional)
    date_pat = r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)"
    try:
        extracted = s.str.extract(date_pat, expand=False)
        # If extraction produced values, prefer extracted substring
        has_extracted = extracted.notna()
        s_pref = s.copy()
        s_pref[has_extracted] = extracted[has_extracted]
    except Exception:
        s_pref = s

    # Try parsing with a few strategies to handle variations
    parsed = pd.to_datetime(s_pref, format='%Y/%m/%d %H:%M:%S', errors='coerce')

    # Try with seconds optional
    mask_na = parsed.isna()
    if mask_na.any():
        parsed2 = pd.to_datetime(s_pref, format='%Y/%m/%d %H:%M', errors='coerce')
        parsed = parsed.fillna(parsed2)

    # General fallback (dateutil / infer)
    mask_na = parsed.isna()
    if mask_na.any():
        try:
            fallback = pd.to_datetime(s_pref, errors='coerce', infer_datetime_format=True)
            parsed = parsed.fillna(fallback)
        except Exception:
            pass

    # As last resort, try dayfirst=True for ambiguous formats
    mask_na = parsed.isna()
    if mask_na.any():
        try:
            fallback2 = pd.to_datetime(s_pref, errors='coerce', dayfirst=True, infer_datetime_format=True)
            parsed = parsed.fillna(fallback2)
        except Exception:
            pass

    return parsed


def extract_date_substring(text):
    """Return a date-like substring from text if present, else None.
    Matches patterns like 'YYYY/MM/DD HH:MM:SS', 'YYYY-MM-DD HH:MM', 'YYYY/MM/DD', etc.
    """
    if text is None:
        return None
    s = str(text)
    # quick cleanup
    s = s.replace('\u00A0', ' ').replace('\u200B', '').replace('\uFEFF', '').strip()
    if not s:
        return None
    import re
    date_pat = r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)"
    m = re.search(date_pat, s)
    if m:
        return m.group(1)
    return None

def save_data(df_one_row, spreadsheet_id, worksheet_name):
    client = get_gspread_client()
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを保存できませんでした。")
        return False
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        current_headers = []
        if worksheet.row_count > 0:
            current_headers = worksheet.row_values(1)
        if not current_headers or len(current_headers) < len(COLUMNS) or current_headers[:len(COLUMNS)] != COLUMNS :
            worksheet.update('A1', [COLUMNS], value_input_option='USER_ENTERED')
            if not current_headers: st.info("スプレッドシートにヘッダー行を書き込みました。")
            else: st.warning("スプレッドシートのヘッダーを修正しました。")
        data_to_append = []
        for col in COLUMNS:
            if col in df_one_row.columns:
                value = df_one_row.iloc[0][col]
                if pd.isna(value):
                    data_to_append.append("")
                elif col == 'timestamp':
                    # When saving new records, store timestamp as date-only (YYYY-MM-DD)
                    # If value is a datetime-like object, format the date part only.
                    if isinstance(value, (datetime, pd.Timestamp)):
                        data_to_append.append(value.strftime('%Y-%m-%d'))
                    else:
                        # If it's a string-like value, try to parse and convert to date-only when possible.
                        val_str = str(value).strip()
                        if val_str:
                            try:
                                parsed_val = pd.to_datetime(val_str, errors='coerce', infer_datetime_format=True)
                                if pd.notna(parsed_val):
                                    data_to_append.append(parsed_val.strftime('%Y-%m-%d'))
                                else:
                                    data_to_append.append(val_str)
                            except Exception:
                                data_to_append.append(val_str)
                        else:
                            data_to_append.append("")
                elif col == 'finish_turn' and pd.notna(value):
                    data_to_append.append(int(value))
                else:
                    data_to_append.append(str(value))
            else:
                data_to_append.append("")
        worksheet.append_row(data_to_append, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Google Sheetsへのデータ書き込み中にエラーが発生しました: {type(e).__name__}: {e}")
        return False

# --- 入力フォーム用ヘルパー関数 (シーズン絞り込み対応) ---
def get_unique_items_with_new_option(df, column_name, predefined_options=None):
    items = []
    if predefined_options is not None:
        items = list(predefined_options)

    if df is not None and not df.empty and column_name in df.columns and not df[column_name].empty:
        valid_items_series = df[column_name].astype(str).replace('', pd.NA).dropna()
        if not valid_items_series.empty:
            unique_valid_items = sorted(valid_items_series.unique().tolist())
            if predefined_options is not None:
                items = sorted(list(set(items + unique_valid_items)))
            else:
                items = unique_valid_items

    final_options = []
    if NEW_ENTRY_LABEL not in items:
        final_options.append(NEW_ENTRY_LABEL)
    final_options.extend([item for item in items if item != NEW_ENTRY_LABEL])
    return final_options

# --- 入力フォーム用ヘルパー関数 (シーズン・クラス・フォーマット絞り込み対応) ---
def get_decks_for_filter_conditions_input(df, selected_season, selected_ui_class, selected_format):
    """
    指定されたシーズン、UIで選択されたクラス、選択されたフォーマットに基づいて、
    my_deck と opponent_deck の両方から該当するユニークなデッキ名のリストを取得する。
    """
    # シーズン、クラス、フォーマットが選択されていない場合は候補を絞り込めない
    if (not selected_season or selected_season == NEW_ENTRY_LABEL or pd.isna(selected_season) or
        not selected_ui_class or
        not selected_format or selected_format == NEW_ENTRY_LABEL):
        return [NEW_ENTRY_LABEL]

    df_filtered = df[
        (df['season'].astype(str) == str(selected_season)) &
        (df['format'].astype(str) == str(selected_format))
    ]
    
    if df_filtered.empty:
        return [NEW_ENTRY_LABEL]

    deck_names_set = set()

    # UIで選択されたクラスが「自分のクラス」列と一致する場合の「自分のデッキ名」を収集
    my_class_deck_df = df_filtered[df_filtered['my_class'].astype(str) == str(selected_ui_class)]
    if not my_class_deck_df.empty and 'my_deck' in my_class_deck_df.columns:
        valid_items_my_deck = my_class_deck_df['my_deck'].astype(str).replace('', pd.NA).dropna()
        deck_names_set.update(d for d in valid_items_my_deck.tolist() if d and d.lower() != 'nan')
    
    # UIで選択されたクラスが「相手のクラス」列と一致する場合の「相手のデッキ名」を収集
    opponent_class_deck_df = df_filtered[df_filtered['opponent_class'].astype(str) == str(selected_ui_class)]
    if not opponent_class_deck_df.empty and 'opponent_deck' in opponent_class_deck_df.columns:
        valid_items_opponent_deck = opponent_class_deck_df['opponent_deck'].astype(str).replace('', pd.NA).dropna()
        deck_names_set.update(d for d in valid_items_opponent_deck.tolist() if d and d.lower() != 'nan')
            
    if not deck_names_set:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(deck_names_set))

def get_types_for_filter_conditions_input(df, selected_season, selected_ui_class, selected_deck_name, selected_format):
    """
    指定されたシーズン、UIで選択されたクラス、UIで選択されたデッキ名、選択されたフォーマットに基づいて、
    my_deck_type と opponent_deck_type の両方から該当するユニークなデッキタイプのリストを取得する。
    """
    # 必須項目が選択されていない場合は候補を絞り込めない
    if (not selected_season or selected_season == NEW_ENTRY_LABEL or pd.isna(selected_season) or
        not selected_ui_class or 
        not selected_deck_name or selected_deck_name == NEW_ENTRY_LABEL or pd.isna(selected_deck_name) or
        not selected_format or selected_format == NEW_ENTRY_LABEL):
        return [NEW_ENTRY_LABEL]

    df_filtered = df[
        (df['season'].astype(str) == str(selected_season)) &
        (df['format'].astype(str) == str(selected_format))
    ]

    if df_filtered.empty:
        return [NEW_ENTRY_LABEL]

    types_set = set()

    # UIで選択されたクラスが「自分のクラス」で、かつ選択されたデッキ名が「自分のデッキ」の場合の「自分のデッキタイプ」を収集
    my_context_df = df_filtered[
        (df_filtered['my_class'].astype(str) == str(selected_ui_class)) &
        (df_filtered['my_deck'].astype(str) == str(selected_deck_name))
    ]
    if not my_context_df.empty and 'my_deck_type' in my_context_df.columns:
        valid_items_my_type = my_context_df['my_deck_type'].astype(str).replace('', pd.NA).dropna()
        types_set.update(t for t in valid_items_my_type.tolist() if t and t.lower() != 'nan')

    # UIで選択されたクラスが「相手のクラス」で、かつ選択されたデッキ名が「相手のデッキ」の場合の「相手のデッキタイプ」を収集
    opponent_context_df = df_filtered[
        (df_filtered['opponent_class'].astype(str) == str(selected_ui_class)) &
        (df_filtered['opponent_deck'].astype(str) == str(selected_deck_name))
    ]
    if not opponent_context_df.empty and 'opponent_deck_type' in opponent_context_df.columns:
        valid_items_opponent_type = opponent_context_df['opponent_deck_type'].astype(str).replace('', pd.NA).dropna()
        types_set.update(t for t in valid_items_opponent_type.tolist() if t and t.lower() != 'nan')

    if not types_set:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(types_set))

# --- 分析用ヘルパー関数 ---
def get_all_analyzable_deck_names(df):
    my_decks = df['my_deck'].astype(str).replace('', pd.NA).dropna().unique()
    all_decks_set = set(my_decks)
    return sorted([d for d in all_decks_set if d and d.lower() != 'nan'])

def get_all_types_for_archetype(df, deck_name):
    if not deck_name or deck_name == SELECT_PLACEHOLDER or pd.isna(deck_name):
        return [ALL_TYPES_PLACEHOLDER]
    types = set()
    my_deck_matches = df[(df['my_deck'].astype(str) == str(deck_name))]
    if not my_deck_matches.empty and 'my_deck_type' in my_deck_matches.columns:
        types.update(my_deck_matches['my_deck_type'].astype(str).replace('', pd.NA).dropna().tolist())
    valid_types = sorted([t for t in list(types) if t and t.lower() != 'nan'])
    return [ALL_TYPES_PLACEHOLDER] + valid_types

# --- 分析セクション表示関数 ---
def display_general_deck_performance(df_to_analyze):
    st.subheader("使用デッキ パフォーマンス概要")
    all_my_deck_archetypes = get_all_analyzable_deck_names(df_to_analyze)
    if not all_my_deck_archetypes:
        st.info("分析可能な使用デッキデータが現在の絞り込み条件ではありません。")
        return

    general_performance_data = []
    for deck_a_name in all_my_deck_archetypes:
        if not deck_a_name: continue

        games_as_my_deck_df = df_to_analyze[df_to_analyze['my_deck'] == deck_a_name]
        if games_as_my_deck_df.empty:
            continue

        wins_as_my_deck = len(games_as_my_deck_df[games_as_my_deck_df['result'] == '勝ち'])
        count_as_my_deck = len(games_as_my_deck_df)

        total_appearances_deck_a = count_as_my_deck
        total_wins_deck_a = wins_as_my_deck
        total_losses_deck_a = total_appearances_deck_a - total_wins_deck_a
        simple_overall_win_rate_deck_a = (total_wins_deck_a / total_appearances_deck_a * 100) if total_appearances_deck_a > 0 else 0.0

        deck_a_first_as_my = games_as_my_deck_df[games_as_my_deck_df['first_second'] == '先攻']
        total_games_deck_a_first = len(deck_a_first_as_my)
        wins_deck_a_first = len(deck_a_first_as_my[deck_a_first_as_my['result'] == '勝ち'])
        win_rate_deck_a_first = (wins_deck_a_first / total_games_deck_a_first * 100) if total_games_deck_a_first > 0 else None

        deck_a_second_as_my = games_as_my_deck_df[games_as_my_deck_df['first_second'] == '後攻']
        total_games_deck_a_second = len(deck_a_second_as_my)
        wins_deck_a_second = len(deck_a_second_as_my[deck_a_second_as_my['result'] == '勝ち'])
        win_rate_deck_a_second = (wins_deck_a_second / total_games_deck_a_second * 100) if total_games_deck_a_second > 0 else None

        matchup_win_rates_for_deck_a = []
        unique_opponents_faced_by_deck_a = set()
        for opponent_deck_name_raw in games_as_my_deck_df['opponent_deck'].unique():
            if opponent_deck_name_raw and str(opponent_deck_name_raw).strip() and str(opponent_deck_name_raw).strip().lower() != 'nan':
                 unique_opponents_faced_by_deck_a.add(str(opponent_deck_name_raw))

        if unique_opponents_faced_by_deck_a:
            for opponent_archetype_name in unique_opponents_faced_by_deck_a:
                a_vs_opp_my_games = games_as_my_deck_df[games_as_my_deck_df['opponent_deck'] == opponent_archetype_name]
                a_vs_opp_my_wins = len(a_vs_opp_my_games[a_vs_opp_my_games['result'] == '勝ち'])
                total_games_vs_specific_opponent = len(a_vs_opp_my_games)
                total_wins_for_a_vs_specific_opponent = a_vs_opp_my_wins

                if total_games_vs_specific_opponent > 0:
                    wr = (total_wins_for_a_vs_specific_opponent / total_games_vs_specific_opponent * 100)
                    matchup_win_rates_for_deck_a.append(wr)
        avg_matchup_wr_deck_a = pd.Series(matchup_win_rates_for_deck_a).mean() if matchup_win_rates_for_deck_a else None

        if total_appearances_deck_a > 0:
            appearance_display = f"{total_appearances_deck_a} (先攻: {total_games_deck_a_first})"
            general_performance_data.append({
                "使用デッキ": deck_a_name, "使用回数": appearance_display,
                "勝利数": total_wins_deck_a, "敗北数": total_losses_deck_a,
                "勝率 (%)": simple_overall_win_rate_deck_a,
                "平均マッチアップ勝率 (%)": avg_matchup_wr_deck_a,
                "先攻時勝率 (%)": win_rate_deck_a_first, "後攻時勝率 (%)": win_rate_deck_a_second,
            })

    if general_performance_data:
        gen_perf_df = pd.DataFrame(general_performance_data)
        default_sort_column = "平均マッチアップ勝率 (%)"
        if default_sort_column not in gen_perf_df.columns: default_sort_column = "勝率 (%)"
        if default_sort_column not in gen_perf_df.columns: default_sort_column = "使用回数"
        try:
            gen_perf_df_sorted = gen_perf_df.sort_values(by=default_sort_column, ascending=False, na_position='last').reset_index(drop=True)
        except KeyError:
            gen_perf_df_sorted = gen_perf_df.reset_index(drop=True)
        except TypeError:
            st.warning(f"列 '{default_sort_column}' でのソートに失敗しました。表示順は保証されません。")
            gen_perf_df_sorted = gen_perf_df.reset_index(drop=True)

        display_cols_general = [
            "使用デッキ", "使用回数", "勝利数", "敗北数",
            "勝率 (%)", "平均マッチアップ勝率 (%)",
            "先攻時勝率 (%)", "後攻時勝率 (%)"
        ]
        actual_display_cols_general = [col for col in display_cols_general if col in gen_perf_df_sorted.columns]
        st.dataframe(gen_perf_df_sorted[actual_display_cols_general].style.format({
            "勝率 (%)": "{:.1f}%",
            "平均マッチアップ勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
            "先攻時勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
            "後攻時勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
        }), use_container_width=True)
    else: st.info("表示する使用デッキのパフォーマンスデータがありません。")

def display_opponent_deck_summary(df_to_analyze):
    st.subheader("対戦相手デッキ傾向分析")

    if df_to_analyze.empty:
        st.info("分析対象のデータがありません。")
        return

    if 'opponent_deck' not in df_to_analyze.columns or df_to_analyze['opponent_deck'].dropna().empty:
        st.info("対戦相手のデッキ情報が記録されていません。")
        return

    valid_opponent_decks = df_to_analyze['opponent_deck'].astype(str).replace('', pd.NA).dropna()
    if valid_opponent_decks.empty:
        st.info("集計可能な対戦相手のデッキ情報がありません。")
        return
        
    opponent_deck_counts = valid_opponent_decks.value_counts().reset_index()
    opponent_deck_counts.columns = ['対戦相手デッキ', '遭遇回数']
    
    total_games_in_scope = len(df_to_analyze)

    summary_data = []
    for index, row in opponent_deck_counts.iterrows():
        opp_deck_name = row['対戦相手デッキ']
        appearances = row['遭遇回数']

        if not opp_deck_name or str(opp_deck_name).lower() == 'nan' or str(opp_deck_name).strip() == "":
            continue

        games_vs_this_opp = df_to_analyze[df_to_analyze['opponent_deck'] == opp_deck_name]
        
        my_wins_vs_opp = len(games_vs_this_opp[games_vs_this_opp['result'] == '勝ち'])
        my_losses_vs_opp = appearances - my_wins_vs_opp
        
        win_rate_vs_opp = (my_wins_vs_opp / appearances * 100) if appearances > 0 else None
        usage_percentage = (appearances / total_games_in_scope * 100) if total_games_in_scope > 0 else 0

        avg_finish_turn_vs_opp = None
        if 'finish_turn' in games_vs_this_opp.columns:
            valid_finish_turns_series_vs_opp = games_vs_this_opp['finish_turn'].dropna().astype(float)
            valid_finish_turns_vs_opp = valid_finish_turns_series_vs_opp[valid_finish_turns_series_vs_opp > 0]
            if not valid_finish_turns_vs_opp.empty:
                avg_finish_turn_vs_opp = valid_finish_turns_vs_opp.mean()
        
        summary_data.append({
            "相手デッキ": opp_deck_name,
            "遭遇回数": appearances,
            "遭遇率 (%)": usage_percentage,
            "自分勝利数": my_wins_vs_opp,
            "自分敗北数": my_losses_vs_opp,
            "自分の勝率 (%)": win_rate_vs_opp,
            "平均決着ターン": avg_finish_turn_vs_opp
        })

    if not summary_data:
        st.info("集計可能な対戦相手デッキの情報がありません。")
        return

    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values(by=["遭遇回数", "自分の勝率 (%)"], ascending=[False, False]).reset_index(drop=True)

    display_cols = ["相手デッキ", "遭遇回数", "遭遇率 (%)", "自分勝利数", "自分敗北数", "自分の勝率 (%)", "平均決着ターン"]
    
    actual_display_cols = [col for col in display_cols if col in summary_df.columns]

    st.dataframe(summary_df[actual_display_cols].style.format({
        "遭遇率 (%)": "{:.1f}%",
        "自分の勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
        "平均決着ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
    }), use_container_width=True)

def display_overall_filtered_performance(df_to_analyze):
    st.subheader("総合戦績概要")

    if df_to_analyze.empty:
        st.info("この条件での分析対象データがありません。")
        return

    total_games = len(df_to_analyze)
    total_wins = len(df_to_analyze[df_to_analyze['result'] == '勝ち'])
    total_losses = total_games - total_wins
    overall_win_rate = (total_wins / total_games * 100) if total_games > 0 else None

    first_games_df = df_to_analyze[df_to_analyze['first_second'] == '先攻']
    total_first_games = len(first_games_df)
    wins_first = len(first_games_df[first_games_df['result'] == '勝ち'])
    win_rate_first = (wins_first / total_first_games * 100) if total_first_games > 0 else None

    second_games_df = df_to_analyze[df_to_analyze['first_second'] == '後攻']
    total_second_games = len(second_games_df)
    wins_second = len(second_games_df[second_games_df['result'] == '勝ち'])
    win_rate_second = (wins_second / total_second_games * 100) if total_second_games > 0 else None
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("総対戦数", f"{total_games} 戦")
        st.metric("先攻時勝率", 
                  f"{win_rate_first:.1f}%" if win_rate_first is not None else "N/A",
                  help=f"先攻 {total_first_games}戦 {wins_first}勝" if total_first_games > 0 else "データなし")
        
    with col2:
        st.metric("総勝利数", f"{total_wins} 勝")
        st.metric("後攻時勝率", 
                  f"{win_rate_second:.1f}%" if win_rate_second is not None else "N/A",
                  help=f"後攻 {total_second_games}戦 {wins_second}勝" if total_second_games > 0 else "データなし")

    with col3:
        st.metric("総敗北数", f"{total_losses} 敗")
        st.metric("総合勝率", f"{overall_win_rate:.1f}%" if overall_win_rate is not None else "N/A")

def show_analysis_section(original_df):
    st.header("📊 戦績分析")
    if original_df.empty:
        st.info("まだ分析できる戦績データがありません。")
        return
    st.subheader("絞り込み条件")
    
    if 'ana_season_filter' not in st.session_state:
        if not original_df.empty and 'season' in original_df.columns:
            last_season = original_df.iloc[-1]['season']
            if pd.notna(last_season) and str(last_season).strip() and str(last_season).lower() != 'nan':
                st.session_state.ana_season_filter = str(last_season)
            else:
                st.session_state.ana_season_filter = SELECT_PLACEHOLDER
        else:
            st.session_state.ana_season_filter = SELECT_PLACEHOLDER

    available_formats_in_data = sorted([
        f for f in original_df['format'].astype(str).replace('', pd.NA).dropna().unique() 
        if f and f.lower() != 'nan'
    ])

    if 'ana_format_filter' not in st.session_state:
        if not original_df.empty and 'format' in original_df.columns:
            last_format = original_df.iloc[-1]['format']
            if pd.notna(last_format) and str(last_format).strip() and str(last_format).lower() != 'nan':
                last_format_str = str(last_format)
                if last_format_str in available_formats_in_data:
                    st.session_state.ana_format_filter = [last_format_str]
                else:
                    if "ローテーション" in available_formats_in_data:
                        st.session_state.ana_format_filter = ["ローテーション"]
                    else:
                        st.session_state.ana_format_filter = []
            else:
                if "ローテーション" in available_formats_in_data:
                    st.session_state.ana_format_filter = ["ローテーション"]
                else:
                    st.session_state.ana_format_filter = []
        else:
            if "ローテーション" in available_formats_in_data:
                st.session_state.ana_format_filter = ["ローテーション"]
            else:
                st.session_state.ana_format_filter = []

    st.markdown("**日付による絞り込み (任意)**")
    
    date_filter_type = st.radio(
        "日付絞り込み方法を選択:",
        ["日付絞り込みなし", "期間指定", "特定日付指定"],
        key='ana_date_filter_type',
        horizontal=True
    )
    
    selected_date_range = None
    selected_specific_dates = None
    
    if date_filter_type == "期間指定":
        if 'timestamp' in original_df.columns:
            parsed_dates = parse_timestamp_series(original_df['timestamp'])
            valid_dates = parsed_dates.dropna()
            if not valid_dates.empty:
                min_date = valid_dates.min().date()
                max_date = valid_dates.max().date()

                col_start, col_end = st.columns(2)
                with col_start:
                    start_date = st.date_input(
                        "開始日", 
                        value=min_date,
                        min_value=min_date,
                        max_value=max_date,
                        key='ana_start_date'
                    )
                with col_end:
                    end_date = st.date_input(
                        "終了日", 
                        value=max_date,
                        min_value=min_date,
                        max_value=max_date,
                        key='ana_end_date'
                    )
                
                if start_date <= end_date:
                    selected_date_range = (start_date, end_date)
                else:
                    st.error("開始日は終了日以前の日付を選択してください。")
            else:
                st.warning("有効な日付データが見つかりません。")
    
    elif date_filter_type == "特定日付指定":
        if 'timestamp' in original_df.columns:
            parsed_dates = parse_timestamp_series(original_df['timestamp'])
            valid_dates = parsed_dates.dropna()
            if not valid_dates.empty:
                if 'selected_dates' not in st.session_state:
                    st.session_state.selected_dates = []

                col1, col2 = st.columns([3, 1])
                with col1:
                    new_date = st.date_input("日付を選択", key="date_selector_for_specific")
                with col2:
                    st.write("\n") # ボタンを中央に配置するためのスペーサー
                    if st.button("日付を追加"):
                        if new_date not in st.session_state.selected_dates:
                            st.session_state.selected_dates.append(new_date)
                            st.session_state.selected_dates.sort()
                
                if st.session_state.selected_dates:
                    st.write("選択中の日付:")
                    selected_dates_str = [d.strftime("%Y-%m-%d") for d in st.session_state.selected_dates]
                    st.write(selected_dates_str)
                    if st.button("選択をクリア"):
                        st.session_state.selected_dates = []
                        st.rerun()
                
                selected_specific_dates = st.session_state.selected_dates

            else:
                st.warning("有効な日付データが見つかりません。")
    
    st.markdown("---")

    # Optional debug: show raw vs parsed samples for columns that look like datetimes
    try:
        debug_ts = st.checkbox("デバッグ: timestamp パースの先頭サンプルを表示", key='debug_timestamp_parsing')
    except Exception:
        debug_ts = False

    all_seasons = [SELECT_PLACEHOLDER] + sorted([s for s in original_df['season'].astype(str).replace('', pd.NA).dropna().unique() if s and s.lower() != 'nan'])
    selected_season_for_analysis = st.selectbox("シーズンで絞り込み (任意):", options=all_seasons, key='ana_season_filter')

    all_environments = [SELECT_PLACEHOLDER] + sorted([e for e in original_df['environment'].astype(str).replace('', pd.NA).dropna().unique() if e and e.lower() != 'nan'])
    selected_environments = st.multiselect("対戦環境で絞り込み (任意):", options=all_environments, key='ana_environment_filter')

    all_formats = [SELECT_PLACEHOLDER] + sorted([f for f in original_df['format'].astype(str).replace('', pd.NA).dropna().unique() if f and f.lower() != 'nan'])
    selected_formats = st.multiselect("フォーマットで絞り込み (任意):", options=all_formats, key='ana_format_filter')

    all_groups = [SELECT_PLACEHOLDER] + sorted([g for g in original_df['group'].astype(str).replace('', pd.NA).dropna().unique() if g and g.lower() != 'nan'])
    selected_groups = st.multiselect("グループで絞り込み (任意):", options=all_groups, key='ana_group_filter')

    df_for_analysis = original_df.copy()

    # If debug requested, display candidates and sample raw->parsed for inspection
    if debug_ts and not original_df.empty:
        try:
            st.write("---")
            st.write("デバッグ: 日時解析の候補カラムと先頭サンプル（raw -> parsed）")
            candidate_cols = []
            for col in original_df.columns:
                try:
                    parsed_col = parse_timestamp_series(original_df[col])
                except Exception:
                    parsed_col = pd.Series([pd.NaT] * len(original_df))
                if parsed_col.dropna().any():
                    candidate_cols.append(col)

            if not candidate_cols:
                st.write("日時らしいカラムは見つかりませんでした。")
            else:
                rows = min(10, len(original_df))
                sample = pd.DataFrame()
                for col in candidate_cols:
                    sample[f"{col} (raw)"] = original_df[col].astype(str).reset_index(drop=True).head(rows)
                    sample[f"{col} (parsed)"] = parse_timestamp_series(original_df[col]).astype(str).reset_index(drop=True).head(rows)
                st.dataframe(sample, width='stretch')
                # Additionally show failing rows from the end (ユーザーの要望: 後ろからデバッグ)
                st.write("")
                st.write("---")
                st.write("デバッグ: 解析に失敗している行（下から最大20件） — raw と parsed を表示します")
                for col in candidate_cols:
                    parsed_col = parse_timestamp_series(original_df[col])
                    mask_fail = parsed_col.isna()
                    if mask_fail.any():
                        failing = original_df.loc[mask_fail, col].astype(str).reset_index()
                        # show last up to 20 failing rows
                        if not failing.empty:
                            last_fail = failing.tail(20).copy()
                            last_fail.columns = ['row_index', f'{col} (raw)']
                            last_fail[f'{col} (parsed)'] = parse_timestamp_series(last_fail[f'{col} (raw)']).astype(str)
                            st.write(f"列: {col} — 解析失敗行 (末尾から)")
                            st.dataframe(last_fail, width='stretch')
                            # Additional option: fetch the actual sheet rows for these indices
                            try:
                                if st.button(f"シート上の該当行を表示: {col}"):
                                    # Attempt to fetch corresponding sheet rows from Google Sheets
                                    try:
                                        client = get_gspread_client()
                                        if client is None:
                                            st.error("Google Sheets クライアントが取得できませんでした。認証を確認してください。")
                                        elif not SPREADSHEET_ID:
                                            st.error("SPREADSHEET_ID が設定されていません。secrets を確認してください。")
                                        else:
                                            try:
                                                sheet = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
                                            except Exception as e_open:
                                                st.error(f"スプレッドシートを開けませんでした: {e_open}")
                                                sheet = None

                                            if sheet is not None:
                                                rows_to_show = []
                                                for ridx in last_fail['row_index'].tolist():
                                                    # Convert dataframe index to sheet row number. Assumes header is row 1 and data starts at row 2.
                                                    sheet_row = int(ridx) + 2
                                                    try:
                                                        row_vals = sheet.row_values(sheet_row)
                                                    except Exception as e_row:
                                                        row_vals = [f"取得エラー: {e_row}"]
                                                    # detect date-like substring inside row_vals elements
                                                    detected = None
                                                    detected_idx = None
                                                    parsed_val = None
                                                    for i, cell in enumerate(row_vals):
                                                        sub = extract_date_substring(cell)
                                                        if sub:
                                                            try:
                                                                pv = pd.to_datetime(sub, errors='coerce', infer_datetime_format=True)
                                                            except Exception:
                                                                pv = pd.NaT
                                                            if not pd.isna(pv):
                                                                detected = sub
                                                                detected_idx = i
                                                                parsed_val = pv
                                                                break
                                                    rows_to_show.append({'sheet_row': sheet_row, 'values': row_vals, 'detected_substring': detected, 'detected_index': detected_idx, 'parsed': parsed_val})

                                                # Display fetched rows with detection results
                                                fetch_df = pd.DataFrame(rows_to_show)
                                                # stringify parsed for display
                                                if 'parsed' in fetch_df.columns:
                                                    fetch_df['parsed'] = fetch_df['parsed'].astype(str)
                                                st.write("シートから取得した生行データ（sheet_row, values, detected_substring, detected_index, parsed）:")
                                                st.dataframe(fetch_df, width='stretch')
                                    except Exception as e_fetch:
                                        st.error(f"シート取得処理でエラー: {e_fetch}")
                            except Exception:
                                pass
        except Exception as e:
            try:
                st.error(f"デバッグ出力でエラーが発生しました: {e}")
            except Exception:
                pass

    # Create a dedicated 'date' column for filtering to avoid timestamp issues.
    if 'timestamp' in df_for_analysis.columns:
        parsed = parse_timestamp_series(df_for_analysis['timestamp'])

        # Fill missing parsed timestamps by scanning other columns in the same row
        missing_mask = parsed.isna()
        if missing_mask.any():
            # iterate over indices where timestamp couldn't be parsed
            for idx in parsed[missing_mask].index:
                # scan columns for a date-like substring
                filled = None
                for col in df_for_analysis.columns:
                    try:
                        cell = df_for_analysis.at[idx, col]
                    except Exception:
                        cell = None
                    date_sub = extract_date_substring(cell)
                    if date_sub:
                        # try to parse the extracted substring
                        try:
                            parsed_val = pd.to_datetime(date_sub, errors='coerce', infer_datetime_format=True)
                        except Exception:
                            parsed_val = pd.NaT
                        if not pd.isna(parsed_val):
                            parsed.at[idx] = parsed_val
                            filled = True
                            break
                # end scanning columns
        df_for_analysis['date_for_filter'] = parsed.dt.date
    
    # 日付による絞り込み
    if date_filter_type == "期間指定" and selected_date_range:
        start_date, end_date = selected_date_range
        if 'date_for_filter' in df_for_analysis.columns:
            # Filter rows where the date is within the selected range
            df_for_analysis = df_for_analysis[
                (df_for_analysis['date_for_filter'] >= start_date) & 
                (df_for_analysis['date_for_filter'] <= end_date)
            ]
    
    elif date_filter_type == "特定日付指定" and selected_specific_dates:
        if 'date_for_filter' in df_for_analysis.columns:
            # Filter rows where the date is in the list of selected dates
            df_for_analysis = df_for_analysis[df_for_analysis['date_for_filter'].isin(selected_specific_dates)]
    
    if selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER:
        df_for_analysis = df_for_analysis[df_for_analysis['season'] == selected_season_for_analysis]
    if selected_environments:
        df_for_analysis = df_for_analysis[df_for_analysis['environment'].isin(selected_environments)]
    if selected_formats:
        df_for_analysis = df_for_analysis[df_for_analysis['format'].isin(selected_formats)]
    if selected_groups:
        df_for_analysis = df_for_analysis[df_for_analysis['group'].isin(selected_groups)]

    if df_for_analysis.empty:
        conditions_applied = []
        if date_filter_type == "期間指定" and selected_date_range:
            conditions_applied.append(f"日付: {selected_date_range[0]} ～ {selected_date_range[1]}")
        elif date_filter_type == "特定日付指定" and selected_specific_dates:
            date_strs = [d.strftime('%Y-%m-%d') for d in selected_specific_dates]
            conditions_applied.append(f"日付: {', '.join(date_strs)}")
        if selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER:
            conditions_applied.append(f"シーズン: {selected_season_for_analysis}")
        if selected_environments:
            conditions_applied.append(f"対戦環境: {', '.join(selected_environments)}")
        if selected_formats:
            conditions_applied.append(f"フォーマット: {', '.join(selected_formats)}")
        if selected_groups:
            conditions_applied.append(f"グループ: {', '.join(selected_groups)}")
        
        if conditions_applied:
            st.warning(f"選択された絞り込み条件に合致するデータがありません。\n適用された条件: {' | '.join(conditions_applied)}")
        else: 
            st.info("分析対象のデータがありません。")
        return

    if date_filter_type != "日付絞り込みなし" or selected_season_for_analysis != SELECT_PLACEHOLDER or selected_environments or selected_formats or selected_groups:
        conditions_summary = []
        if date_filter_type == "期間指定" and selected_date_range:
            conditions_summary.append(f"📅 {selected_date_range[0]} ～ {selected_date_range[1]}")
        elif date_filter_type == "特定日付指定" and selected_specific_dates:
            if len(selected_specific_dates) <= 3:
                date_strs = [d.strftime('%Y-%m-%d') for d in selected_specific_dates]
                conditions_summary.append(f"📅 {', '.join(date_strs)}")
            else:
                conditions_summary.append(f"📅 {len(selected_specific_dates)}日分のデータ")
        if selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER:
            conditions_summary.append(f"🏆 {selected_season_for_analysis}")
        if selected_environments:
            conditions_summary.append(f"🎮 {', '.join(selected_environments)}")
        if selected_formats:
            conditions_summary.append(f"📋 {', '.join(selected_formats)}")
        if selected_groups:
            conditions_summary.append(f"💎 {', '.join(selected_groups)}")
        
        if conditions_summary:
            st.info(f"絞り込み条件: {' | '.join(conditions_summary)} | 対象データ: {len(df_for_analysis)}件")

    st.subheader("使用デッキ詳細分析")
    def reset_focus_type_callback():
        st.session_state.ana_focus_deck_type_selector = ALL_TYPES_PLACEHOLDER
        if 'inp_ana_focus_deck_type_new' in st.session_state:
            st.session_state.inp_ana_focus_deck_type_new = ""

    deck_names_for_focus_options = [SELECT_PLACEHOLDER] + get_all_analyzable_deck_names(df_for_analysis)
    st.selectbox("分析する使用デッキアーキタイプを選択:", options=deck_names_for_focus_options, key='ana_focus_deck_name_selector', on_change=reset_focus_type_callback)
    selected_focus_deck = st.session_state.get('ana_focus_deck_name_selector')

    if selected_focus_deck and selected_focus_deck != SELECT_PLACEHOLDER:
        types_for_focus_deck_options = get_all_types_for_archetype(df_for_analysis, selected_focus_deck)
        st.selectbox("使用デッキの型を選択 (「全タイプ」で型を問わず集計):", options=types_for_focus_deck_options, key='ana_focus_deck_type_selector')
        selected_focus_type = st.session_state.get('ana_focus_deck_type_selector')
        st.markdown("---")
        focus_deck_display_name = f"{selected_focus_deck}"
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            focus_deck_display_name += f" ({selected_focus_type})"
        st.subheader(f"「{focus_deck_display_name}」使用時の分析結果")

        cond_my_deck_focus = (df_for_analysis['my_deck'] == selected_focus_deck)
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            cond_my_deck_focus &= (df_for_analysis['my_deck_type'] == selected_focus_type)
        focus_as_my_deck_games = df_for_analysis[cond_my_deck_focus]

        total_appearances = len(focus_as_my_deck_games)
        if total_appearances == 0:
            st.warning(f"「{focus_deck_display_name}」の使用記録が現在の絞り込み条件で見つかりません。")
            return

        wins_when_focus_is_my_deck_df = focus_as_my_deck_games[focus_as_my_deck_games['result'] == '勝ち']
        total_wins_for_focus_deck = len(wins_when_focus_is_my_deck_df)

        win_rate_for_focus_deck = (total_wins_for_focus_deck / total_appearances * 100) if total_appearances > 0 else 0.0

        win_finish_turns = []
        if not wins_when_focus_is_my_deck_df.empty and 'finish_turn' in wins_when_focus_is_my_deck_df.columns:
            valid_turns = wins_when_focus_is_my_deck_df['finish_turn'].dropna().astype(float)
            win_finish_turns.extend(valid_turns[valid_turns > 0].tolist())
        avg_win_finish_turn_val = pd.Series(win_finish_turns).mean() if win_finish_turns else None

        focus_first_my = focus_as_my_deck_games[focus_as_my_deck_games['first_second'] == '先攻']
        total_games_focus_first = len(focus_first_my)
        wins_focus_first = len(focus_first_my[focus_first_my['result'] == '勝ち'])
        win_rate_focus_first = (wins_focus_first / total_games_focus_first * 100) if total_games_focus_first > 0 else None

        focus_second_my = focus_as_my_deck_games[focus_as_my_deck_games['first_second'] == '後攻']
        total_games_focus_second = len(focus_second_my)
        wins_focus_second = len(focus_second_my[focus_second_my['result'] == '勝ち'])
        win_rate_focus_second = (wins_focus_second / total_games_focus_second * 100) if total_games_focus_second > 0 else None

        st.markdown("**総合パフォーマンス (使用者視点)**")
        perf_col1, perf_col2, perf_col3 = st.columns(3)
        with perf_col1:
            st.metric("総使用回数", total_appearances)
            st.metric("先攻時勝率", f"{win_rate_focus_first:.1f}%" if win_rate_focus_first is not None else "N/A",
                      help=f"先攻時 {wins_focus_first}勝 / {total_games_focus_first}戦" if total_games_focus_first > 0 else "データなし")
        with perf_col2:
            st.metric("総勝利数", total_wins_for_focus_deck)
            st.metric("後攻時勝率", f"{win_rate_focus_second:.1f}%" if win_rate_focus_second is not None else "N/A",
                      help=f"後攻時 {wins_focus_second}勝 / {total_games_focus_second}戦" if total_games_focus_second > 0 else "データなし")
        with perf_col3:
            st.metric("総合勝率", f"{win_rate_for_focus_deck:.1f}%")
            st.metric("勝利時平均ターン", f"{avg_win_finish_turn_val:.1f} T" if avg_win_finish_turn_val is not None else "N/A")

        st.markdown("---")
        st.subheader(f"「{focus_deck_display_name}」使用時の対戦相手傾向")

        if not focus_as_my_deck_games.empty:
            opponent_deck_summary_list = []
            unique_opponent_archetypes = focus_as_my_deck_games['opponent_deck'].dropna().unique()

            for opp_arch in unique_opponent_archetypes:
                if not opp_arch or str(opp_arch).lower() == 'nan':
                    continue

                games_vs_this_archetype = focus_as_my_deck_games[focus_as_my_deck_games['opponent_deck'] == opp_arch]
                count = len(games_vs_this_archetype)
                wins = len(games_vs_this_archetype[games_vs_this_archetype['result'] == '勝ち'])
                losses = count - wins
                win_rate = (wins / count * 100) if count > 0 else None
                usage_rate = (count / total_appearances * 100) if total_appearances > 0 else 0

                opponent_deck_summary_list.append({
                    "対戦相手デッキ": opp_arch,
                    "登場回数": count,
                    "使用率 (%)": usage_rate,
                    "勝利数": wins,
                    "敗北数": losses,
                    "勝率 (%)": win_rate
                })

            if opponent_deck_summary_list:
                opponent_summary_df = pd.DataFrame(opponent_deck_summary_list)
                opponent_summary_df = opponent_summary_df.sort_values(by="登場回数", ascending=False).reset_index(drop=True)

                st.dataframe(
                    opponent_summary_df.style.format({
                        "使用率 (%)": "{:.1f}%",
                        "勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
                    }),
                    use_container_width=True,
                    column_config={
                        "対戦相手デッキ": st.column_config.TextColumn("対戦相手デッキ"),
                        "登場回数": st.column_config.NumberColumn("登場回数", help="このデッキを相手にした回数"),
                        "使用率 (%)": st.column_config.NumberColumn("遭遇率 (%)", help=f"「{focus_deck_display_name}」使用時の全対戦における、この相手デッキとの遭遇率"),
                        "勝利数": st.column_config.NumberColumn("勝利数"),
                        "敗北数": st.column_config.NumberColumn("敗北数"),
                        "勝率 (%)": st.column_config.NumberColumn("対戦勝率 (%)", help="この相手デッキに対する勝率")
                    }
                )
            else:
                st.info("集計可能な対戦相手デッキの情報がありません。")
        else:
            st.info(f"「{focus_deck_display_name}」の対戦記録がないため、相手デッキの使用傾向を表示できません。")
        st.markdown("**対戦相手別パフォーマンス（相性）**")
        matchup_data = []
        opponents_set = set()
        if not focus_as_my_deck_games.empty:
            for _, row in focus_as_my_deck_games[['opponent_deck', 'opponent_deck_type']].drop_duplicates().iterrows():
                opponents_set.add((str(row['opponent_deck']), str(row['opponent_deck_type'])))

        all_faced_opponents_tuples = sorted(list(opp_tuple for opp_tuple in opponents_set if opp_tuple[0] and opp_tuple[0].lower() != 'nan'))

        for opp_deck_name, opp_deck_type in all_faced_opponents_tuples:
            games_played_count = 0; focus_deck_wins_count = 0
            focus_deck_win_turns_vs_opp = []; focus_deck_loss_turns_vs_opp = []
            fd_vs_opp_first_games_count = 0; fd_vs_opp_first_wins_count = 0
            fd_vs_opp_second_games_count = 0; fd_vs_opp_second_wins_count = 0

            case1_games = focus_as_my_deck_games[
                (focus_as_my_deck_games['opponent_deck'] == opp_deck_name) &
                (focus_as_my_deck_games['opponent_deck_type'] == opp_deck_type)
            ]
            games_played_count += len(case1_games)
            case1_wins_df = case1_games[case1_games['result'] == '勝ち']
            case1_losses_df = case1_games[case1_games['result'] == '負け']
            focus_deck_wins_count += len(case1_wins_df)
            if not case1_wins_df.empty and 'finish_turn' in case1_wins_df.columns:
                valid_win_turns = case1_wins_df['finish_turn'].dropna().astype(float)
                focus_deck_win_turns_vs_opp.extend(valid_win_turns[valid_win_turns > 0].tolist())
            if not case1_losses_df.empty and 'finish_turn' in case1_losses_df.columns:
                valid_loss_turns = case1_losses_df['finish_turn'].dropna().astype(float)
                focus_deck_loss_turns_vs_opp.extend(valid_loss_turns[valid_loss_turns > 0].tolist())

            c1_fd_first = case1_games[case1_games['first_second'] == '先攻']
            fd_vs_opp_first_games_count += len(c1_fd_first)
            fd_vs_opp_first_wins_count += len(c1_fd_first[c1_fd_first['result'] == '勝ち'])

            c1_fd_second = case1_games[case1_games['first_second'] == '後攻']
            fd_vs_opp_second_games_count += len(c1_fd_second)
            fd_vs_opp_second_wins_count += len(c1_fd_second[c1_fd_second['result'] == '勝ち'])

            if games_played_count > 0:
                win_rate_vs_opp = (focus_deck_wins_count / games_played_count * 100)
                avg_win_turn = pd.Series(focus_deck_win_turns_vs_opp).mean() if focus_deck_win_turns_vs_opp else None
                avg_loss_turn = pd.Series(focus_deck_loss_turns_vs_opp).mean() if focus_deck_loss_turns_vs_opp else None
                win_rate_fd_first_vs_opp = (fd_vs_opp_first_wins_count / fd_vs_opp_first_games_count * 100) if fd_vs_opp_first_games_count > 0 else None
                win_rate_fd_second_vs_opp = (fd_vs_opp_second_wins_count / fd_vs_opp_second_games_count * 100) if fd_vs_opp_second_games_count > 0 else None
                games_played_display = f"{games_played_count} (自分の先攻: {fd_vs_opp_first_games_count})"

                matchup_data.append({
                    "対戦相手デッキ": opp_deck_name, "対戦相手デッキの型": opp_deck_type,
                    "対戦数": games_played_display, "(自分の)勝利数": focus_deck_wins_count,
                    "(自分の)勝率(%)": win_rate_vs_opp,
                    "勝利時平均ターン": avg_win_turn, "敗北時平均決着ターン": avg_loss_turn,
                    "(自分の)先攻時勝率(%)": win_rate_fd_first_vs_opp, "(自分の)後攻時勝率(%)": win_rate_fd_second_vs_opp
                })

        if matchup_data:
            matchup_df_specific_types = pd.DataFrame(matchup_data)
            agg_matchup_data = []
            for opp_deck_name_agg in matchup_df_specific_types['対戦相手デッキ'].unique():
                case1_agg_games_total = focus_as_my_deck_games[focus_as_my_deck_games['opponent_deck'] == opp_deck_name_agg]
                total_games_vs_opp_deck_agg = len(case1_agg_games_total)

                focus_wins_agg1_df = case1_agg_games_total[case1_agg_games_total['result'] == '勝ち']
                total_focus_wins_vs_opp_deck_agg = len(focus_wins_agg1_df)
                win_rate_vs_opp_deck_agg = (total_focus_wins_vs_opp_deck_agg / total_games_vs_opp_deck_agg * 100) if total_games_vs_opp_deck_agg > 0 else 0.0

                focus_losses_agg1_df = case1_agg_games_total[case1_agg_games_total['result'] == '負け']
                all_win_turns_agg = []
                if not focus_wins_agg1_df.empty and 'finish_turn' in focus_wins_agg1_df.columns:
                    valid_all_win_turns = focus_wins_agg1_df['finish_turn'].dropna().astype(float)
                    all_win_turns_agg.extend(valid_all_win_turns[valid_all_win_turns > 0].tolist())
                all_loss_turns_agg = []
                if not focus_losses_agg1_df.empty and 'finish_turn' in focus_losses_agg1_df.columns:
                    valid_all_loss_turns = focus_losses_agg1_df['finish_turn'].dropna().astype(float)
                    all_loss_turns_agg.extend(valid_all_loss_turns[valid_all_loss_turns > 0].tolist())

                avg_win_turn_agg = pd.Series(all_win_turns_agg).mean() if all_win_turns_agg else None
                avg_loss_turn_agg = pd.Series(all_loss_turns_agg).mean() if all_loss_turns_agg else None

                c1_fd_first_agg_total = case1_agg_games_total[case1_agg_games_total['first_second'] == '先攻']
                fd_first_games_agg_total_count = len(c1_fd_first_agg_total)
                fd_first_wins_agg_total = len(c1_fd_first_agg_total[c1_fd_first_agg_total['result'] == '勝ち'])
                win_rate_fd_first_agg_total = (fd_first_wins_agg_total / fd_first_games_agg_total_count * 100) if fd_first_games_agg_total_count > 0 else None

                c1_fd_second_agg_total = case1_agg_games_total[case1_agg_games_total['first_second'] == '後攻']
                fd_second_games_agg_total_count = len(c1_fd_second_agg_total)
                fd_second_wins_agg_total = len(c1_fd_second_agg_total[c1_fd_second_agg_total['result'] == '勝ち'])
                win_rate_fd_second_agg_total = (fd_second_wins_agg_total / fd_second_games_agg_total_count * 100) if fd_second_games_agg_total_count > 0 else None

                games_played_display_agg = f"{total_games_vs_opp_deck_agg} (自分の先攻: {fd_first_games_agg_total_count})"
                if total_games_vs_opp_deck_agg > 0:
                    agg_matchup_data.append({
                        "対戦相手デッキ": opp_deck_name_agg, "対戦相手デッキの型": ALL_TYPES_PLACEHOLDER,
                        "対戦数": games_played_display_agg, "(自分の)勝利数": total_focus_wins_vs_opp_deck_agg,
                        "(自分の)勝率(%)": win_rate_vs_opp_deck_agg,
                        "勝利時平均ターン": avg_win_turn_agg, "敗北時平均ターン": avg_loss_turn_agg,
                        "(自分の)先攻時勝率(%)": win_rate_fd_first_agg_total, "(自分の)後攻時勝率(%)": win_rate_fd_second_agg_total
                    })
            matchup_df_all_types = pd.DataFrame(agg_matchup_data)
            matchup_df_combined = pd.concat([matchup_df_specific_types, matchup_df_all_types], ignore_index=True)
            if not matchup_df_combined.empty:
                matchup_df_combined['__sort_type'] = matchup_df_combined['対戦相手デッキの型'].apply(lambda x: ('0_AllTypes' if x == ALL_TYPES_PLACEHOLDER else '1_' + str(x)))
                matchup_df_final = matchup_df_combined.sort_values(by=["対戦相手デッキ", "__sort_type"]).drop(columns=['__sort_type']).reset_index(drop=True)
                st.dataframe(matchup_df_final.style.format({
                    "(自分の)勝率(%)": "{:.1f}%",
                    "勝利時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "敗北時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "(自分の)先攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
                    "(自分の)後攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A"
                }), use_container_width=True)
            else: st.info(f"「{focus_deck_display_name}」使用時の対戦相手別の記録が見つかりません。")
        else: st.info(f"「{focus_deck_display_name}」使用時の対戦相手別の記録が見つかりません。")

        st.markdown("---")
        st.subheader(f"📝 「{focus_deck_display_name}」使用時のメモ付き対戦記録")
        memo_filter_my_deck = (focus_as_my_deck_games['memo'].astype(str).str.strip() != '') & \
                              (focus_as_my_deck_games['memo'].astype(str).str.lower() != 'nan')
        memos_when_my_deck = focus_as_my_deck_games[memo_filter_my_deck]
        all_memo_games = memos_when_my_deck.reset_index(drop=True)

        if not all_memo_games.empty:
            memo_display_cols = ['timestamp', 'season', 'environment', 'format', 'my_deck', 'my_deck_type', 'opponent_deck', 'opponent_deck_type', 'first_second', 'result', 'finish_turn', 'memo']
            actual_memo_display_cols = [col for col in memo_display_cols if col in all_memo_games.columns]
            df_memo_display = all_memo_games[actual_memo_display_cols].copy()
            if 'timestamp' in df_memo_display.columns:
                df_memo_display['timestamp'] = pd.to_datetime(df_memo_display['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(df_memo_display.sort_values(by='timestamp', ascending=False), use_container_width=True)
        else: st.info(f"「{focus_deck_display_name}」使用時のメモ付きの記録は、現在の絞り込み条件ではありません。")
    else:
        display_overall_filtered_performance(df_for_analysis)
        display_general_deck_performance(df_for_analysis)
        display_opponent_deck_summary(df_for_analysis)

# --- Streamlit アプリ本体 (main関数) ---
def main():
    PREDEFINED_CLASSES = ["エルフ", "ロイヤル", "ウィッチ", "ドラゴン", "ナイトメア", "ビショップ", "ネメシス"]

    st.title(f"{SPREADSHEET_NAME_DISPLAY}")

    if SPREADSHEET_ID == "ここに実際の Waic-戦績 のスプレッドシートIDを貼り付け":
        st.error("コード内の SPREADSHEET_ID を、お使いのGoogleスプレッドシートの実際のIDに置き換えてください。")
        st.warning("スプレッドシートIDは、スプレッドシートのURLに含まれる長い英数字の文字列です。")
        st.code("https://docs.google.com/spreadsheets/d/【この部分がIDです】/edit")
        st.stop()

    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        try:
            # Do NOT store the raw password in cookies. Use a simple persistent auth flag instead.
            stored_auth_flag = cookies.get('auth')
            if stored_auth_flag and stored_auth_flag == '1':
                st.session_state.authenticated = True
        except Exception as e:
            st.warning(f"クッキーの読み取り中にエラーが発生しました: {e}")
            pass

    if not st.session_state.authenticated:
        st.title("アプリへのログイン")
        login_col1, login_col2, login_col3 = st.columns([1,1,1])
        with login_col2:
            with st.form("login_form_main"):
                st.markdown("#### パスワードを入力してください")
                password_input = st.text_input("パスワード", type="password", key="password_input_field_main", label_visibility="collapsed")
                login_button = st.form_submit_button("ログイン")
                if login_button:
                    if password_input == CORRECT_PASSWORD:
                        st.session_state.authenticated = True
                        # Store only a simple auth flag in the encrypted cookie. Never store the password itself.
                        cookies['auth'] = '1'
                        cookies.save()
                        st.rerun()
                    else:
                        st.error("パスワードが正しくありません。")
        st.stop()

    df = load_data(SPREADSHEET_ID, WORKSHEET_NAME)

    if not st.session_state.get('form_values_initialized_from_gsheet', False):
        if not df.empty:
            last_entry = df.iloc[-1].copy()

            fields_to_load_from_gsheet = {
                'inp_season_select': 'season',
                'inp_environment_select': 'environment',
                'inp_format_select': 'format',
                'inp_group_select': 'group',
                'inp_my_class': 'my_class',
                'inp_my_deck': 'my_deck',
                'inp_my_deck_type': 'my_deck_type',
                'inp_opponent_class': 'opponent_class',
                'inp_opponent_deck': 'opponent_deck',
                'inp_opponent_deck_type': 'opponent_deck_type',
                'inp_first_second': 'first_second',
                'inp_result': 'result',
                'inp_finish_turn': 'finish_turn'
            }

            for session_key, df_col_name in fields_to_load_from_gsheet.items():
                if df_col_name in last_entry and pd.notna(last_entry[df_col_name]):
                    value_from_sheet = last_entry[df_col_name]
                    
                    if session_key == 'inp_finish_turn':
                        if pd.notna(value_from_sheet):
                            try:
                                st.session_state[session_key] = int(value_from_sheet)
                            except (ValueError, TypeError):
                                pass
                    else:
                        st.session_state[session_key] = str(value_from_sheet)
        
        st.session_state.form_values_initialized_from_gsheet = True

    def on_season_select_change_input_form():
        keys_to_reset_options = [
            'inp_my_deck', 'inp_my_deck_type',
            'inp_opponent_deck', 'inp_opponent_deck_type',
        ]
        keys_to_reset_new_fields = [
            'inp_my_deck_new', 'inp_my_deck_type_new',
            'inp_opponent_deck_new', 'inp_opponent_deck_type_new'
        ]
        for key in keys_to_reset_options:
            if key in st.session_state: st.session_state[key] = NEW_ENTRY_LABEL
        for key in keys_to_reset_new_fields:
            if key in st.session_state: st.session_state[key] = ""

    def on_my_class_select_change_input_form():
        if 'inp_my_deck' in st.session_state: st.session_state.inp_my_deck = NEW_ENTRY_LABEL
        if 'inp_my_deck_new' in st.session_state: st.session_state.inp_my_deck_new = ""
        if 'inp_my_deck_type' in st.session_state: st.session_state.inp_my_deck_type = NEW_ENTRY_LABEL
        if 'inp_my_deck_type_new' in st.session_state: st.session_state.inp_my_deck_type_new = ""
        
    def on_opponent_class_select_change_input_form():
        if 'inp_opponent_deck' in st.session_state: st.session_state.inp_opponent_deck = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_new' in st.session_state: st.session_state.inp_opponent_deck_new = ""
        if 'inp_opponent_deck_type' in st.session_state: st.session_state.inp_opponent_deck_type = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_type_new' in st.session_state: st.session_state.inp_opponent_deck_type_new = ""

    def on_my_deck_select_change_input_form():
        if 'inp_my_deck_type' in st.session_state: st.session_state.inp_my_deck_type = NEW_ENTRY_LABEL
        if 'inp_my_deck_type_new' in st.session_state: st.session_state.inp_my_deck_type_new = ""

    def on_opponent_deck_select_change_input_form():
        if 'inp_opponent_deck_type' in st.session_state: st.session_state.inp_opponent_deck_type = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_type_new' in st.session_state: st.session_state.inp_opponent_deck_type_new = ""

    def on_format_select_change_input_form():
        keys_to_reset_options = [
            'inp_my_deck', 'inp_my_deck_type',
            'inp_opponent_deck', 'inp_opponent_deck_type',
        ]
        keys_to_reset_new_fields = [
            'inp_my_deck_new', 'inp_my_deck_type_new',
            'inp_opponent_deck_new', 'inp_opponent_deck_type_new'
        ]
        for key in keys_to_reset_options:
            if key in st.session_state: st.session_state[key] = NEW_ENTRY_LABEL
        for key in keys_to_reset_new_fields:
            if key in st.session_state: st.session_state[key] = ""

    with st.expander("戦績を入力する", expanded=True):
        st.subheader("対戦情報")
        season_options_input = get_unique_items_with_new_option(df, 'season')
        st.selectbox("シーズン *", season_options_input, key='inp_season_select',
                     help="例: 2025前期, 〇〇カップ", on_change=on_season_select_change_input_form)
        if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいシーズン名を入力 *", value=st.session_state.get('inp_season_new', ""), key='inp_season_new')

        predefined_environments = ["ランクマッチ", "レート", "壁打ち"]
        environment_options_input = get_unique_items_with_new_option(df, 'environment', predefined_options=predefined_environments)
        st.selectbox("対戦環境 *", environment_options_input, key='inp_environment_select')
        if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL:
            st.text_input("新しい対戦環境を入力 *", value=st.session_state.get('inp_environment_new', ""), key='inp_environment_new')

        predefined_formats = ["ローテーション", "アンリミテッド", "2Pick"]
        format_options_input = get_unique_items_with_new_option(df, 'format', predefined_options=predefined_formats)
        st.selectbox("フォーマット *", format_options_input, key='inp_format_select', 
                     on_change=on_format_select_change_input_form) 
        if st.session_state.get('inp_format_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいフォーマット名を入力 *", value=st.session_state.get('inp_format_new', ""), key='inp_format_new')

        predefined_groups = ["エメラルド", "トパーズ", "ルビー", "サファイア", "ダイヤモンド"]
        group_options_input = get_unique_items_with_new_option(df, 'group', predefined_options=predefined_groups)
        st.selectbox("グループ *", group_options_input, key='inp_group_select')
        if st.session_state.get('inp_group_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいグループ名を入力 *", value=st.session_state.get('inp_group_new', ""), key='inp_group_new')

        current_selected_season_input = st.session_state.get('inp_season_select')
        current_selected_format_value = st.session_state.get('inp_format_select')
        if current_selected_format_value == NEW_ENTRY_LABEL:
            current_selected_format_value = st.session_state.get('inp_format_new', '')
        
        is_2pick_format = (current_selected_format_value == "2Pick")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("自分のデッキ")
            
            my_class_default_index = 0
            if 'inp_my_class' in st.session_state and st.session_state.inp_my_class in PREDEFINED_CLASSES:
                my_class_default_index = PREDEFINED_CLASSES.index(st.session_state.inp_my_class)
            st.selectbox("自分のクラス *", PREDEFINED_CLASSES, key='inp_my_class',
                         index=my_class_default_index,
                         on_change=on_my_class_select_change_input_form)
            current_my_class_input = st.session_state.get('inp_my_class')

            my_deck_name_options_input = get_decks_for_filter_conditions_input(df, current_selected_season_input, current_my_class_input, current_selected_format_value)
            st.selectbox("使用デッキ *", my_deck_name_options_input, key='inp_my_deck', 
                         on_change=on_my_deck_select_change_input_form, 
                         disabled=is_2pick_format)
            if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL and not is_2pick_format:
                st.text_input("新しい使用デッキ名を入力 *", value=st.session_state.get('inp_my_deck_new', ""), key='inp_my_deck_new', disabled=is_2pick_format)
            current_my_deck_name_input = st.session_state.get('inp_my_deck')

            my_deck_type_options_input = get_types_for_filter_conditions_input(df, current_selected_season_input, current_my_class_input, current_my_deck_name_input, current_selected_format_value)
            st.selectbox("使用デッキの型 *", my_deck_type_options_input, key='inp_my_deck_type', 
                         disabled=is_2pick_format)
            if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL and not is_2pick_format:
                st.text_input("新しい使用デッキの型を入力 *", value=st.session_state.get('inp_my_deck_type_new', ""), key='inp_my_deck_type_new', disabled=is_2pick_format)

        with col2:
            st.subheader("対戦相手のデッキ")

            opponent_class_default_index = 0
            if 'inp_opponent_class' in st.session_state and st.session_state.inp_opponent_class in PREDEFINED_CLASSES:
                opponent_class_default_index = PREDEFINED_CLASSES.index(st.session_state.inp_opponent_class)
            st.selectbox("相手のクラス *", PREDEFINED_CLASSES, key='inp_opponent_class',
                         index=opponent_class_default_index,
                         on_change=on_opponent_class_select_change_input_form)
            current_opponent_class_input = st.session_state.get('inp_opponent_class')
            
            opponent_deck_name_options_input = get_decks_for_filter_conditions_input(df, current_selected_season_input, current_opponent_class_input, current_selected_format_value)
            st.selectbox("相手デッキ *", opponent_deck_name_options_input, key='inp_opponent_deck', 
                         on_change=on_opponent_deck_select_change_input_form, 
                         disabled=is_2pick_format)
            if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL and not is_2pick_format:
                st.text_input("新しい相手デッキ名を入力 *", value=st.session_state.get('inp_opponent_deck_new', ""), key='inp_opponent_deck_new', disabled=is_2pick_format)
            current_opponent_deck_name_input = st.session_state.get('inp_opponent_deck')

            opponent_deck_type_options_input = get_types_for_filter_conditions_input(df, current_selected_season_input, current_opponent_class_input, current_opponent_deck_name_input, current_selected_format_value)
            st.selectbox("相手デッキの型 *", opponent_deck_type_options_input, key='inp_opponent_deck_type', 
                         disabled=is_2pick_format)
            if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL and not is_2pick_format:
                st.text_input("新しい相手デッキの型を入力 *", value=st.session_state.get('inp_opponent_deck_type_new', ""), key='inp_opponent_deck_type_new', disabled=is_2pick_format)
        
        st.subheader("対戦結果")
        res_col1, res_col2, res_col3 = st.columns(3)
        with res_col1:
            first_second_options = ["先攻", "後攻"]
            first_second_default_index = 0
            if 'inp_first_second' in st.session_state and st.session_state.inp_first_second in first_second_options:
                first_second_default_index = first_second_options.index(st.session_state.inp_first_second)
            st.selectbox("自分の先攻/後攻 *", first_second_options, key='inp_first_second', index=first_second_default_index)
        
        with res_col2:
            result_options = ["勝ち", "負け"]
            result_default_index = 0
            if 'inp_result' in st.session_state and st.session_state.inp_result in result_options:
                result_default_index = result_options.index(st.session_state.inp_result)
            st.selectbox("勝敗 *", result_options, key='inp_result', index=result_default_index)
        
        with res_col3:
            st.number_input("決着ターン *", min_value=0, step=1, value=st.session_state.get('inp_finish_turn', 7), placeholder="ターン数を入力", key='inp_finish_turn',help="0はリタイア")
        
        st.text_area("対戦メモ (任意)", value=st.session_state.get('inp_memo', ""), key='inp_memo')

        st.markdown("---")
        error_placeholder = st.empty()
        success_placeholder = st.empty()

        if st.button("戦績を記録", key='submit_record_button'):
            final_season = st.session_state.get('inp_season_new', '') if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_season_select', '')
            if final_season == NEW_ENTRY_LABEL: 
                final_season = ''

            final_environment = st.session_state.get('inp_environment_new', '') if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_environment_select', '')
            if final_environment == NEW_ENTRY_LABEL: 
                final_environment = ''

            final_format = st.session_state.get('inp_format_new', '') if st.session_state.get('inp_format_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_format_select', '')
            if final_format == NEW_ENTRY_LABEL: 
                final_format = ''

            final_group = st.session_state.get('inp_group_new', '') if st.session_state.get('inp_group_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_group_select', '')
            if final_group == NEW_ENTRY_LABEL: 
                final_group = ''

            is_2pick_submit_time = (final_format == "2Pick")
            
            if is_2pick_submit_time:
                final_my_deck = "2Pickデッキ"
                final_my_deck_type = "2Pick"
                final_opponent_deck = "2Pickデッキ"
                final_opponent_deck_type = "2Pick"
            else:
                final_my_deck = st.session_state.get('inp_my_deck_new', '') if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck', '')
                if final_my_deck == NEW_ENTRY_LABEL: 
                    final_my_deck = ''
                final_my_deck_type = st.session_state.get('inp_my_deck_type_new', '') if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck_type', '')
                if final_my_deck_type == NEW_ENTRY_LABEL: 
                    final_my_deck_type = ''
                final_opponent_deck = st.session_state.get('inp_opponent_deck_new', '') if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck', '')
                if final_opponent_deck == NEW_ENTRY_LABEL: 
                    final_opponent_deck = ''
                final_opponent_deck_type = st.session_state.get('inp_opponent_deck_type_new', '') if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck_type', '')
                if final_opponent_deck_type == NEW_ENTRY_LABEL: 
                    final_opponent_deck_type = ''

            final_my_class = st.session_state.get('inp_my_class', '')
            final_opponent_class = st.session_state.get('inp_opponent_class', '')

            timestamp_val = datetime.now()
            first_second_val = st.session_state.get('inp_first_second', '')
            result_val = st.session_state.get('inp_result', '')
            finish_turn_val = st.session_state.get('inp_finish_turn')
            memo_val = st.session_state.get('inp_memo', '')

            error_messages = []
            if not final_season: 
                error_messages.append("シーズンを入力または選択してください。")
            if not final_environment: 
                error_messages.append("対戦環境を選択または入力してください。")
            if not final_format: 
                error_messages.append("フォーマットを選択または入力してください。")
            if not final_group: 
                error_messages.append("グループを選択または入力してください。")
            if not final_my_class: 
                error_messages.append("自分のクラスを選択してください。")
            if not final_opponent_class: 
                error_messages.append("相手のクラスを選択してください。")

            if not is_2pick_submit_time:
                if not final_my_deck: 
                    error_messages.append("使用デッキ名を入力または選択してください。")
                if not final_my_deck_type: 
                    error_messages.append("使用デッキの型を入力または選択してください。")
                if not final_opponent_deck: 
                    error_messages.append("相手デッキ名を入力または選択してください。")
                if not final_opponent_deck_type: 
                    error_messages.append("相手デッキの型を入力または選択してください。")
            
            if finish_turn_val is None: 
                error_messages.append("決着ターンを入力してください。")

            if error_messages:
                error_placeholder.error("、".join(error_messages))
                success_placeholder.empty()
            else:
                error_placeholder.empty()
                new_record_data = {
                    'season': final_season, 'timestamp': timestamp_val,
                    'environment': final_environment, 'format': final_format, 'group': final_group,
                    'my_deck': final_my_deck, 'my_deck_type': final_my_deck_type,
                    'my_class': final_my_class,
                    'opponent_deck': final_opponent_deck, 'opponent_deck_type': final_opponent_deck_type,
                    'opponent_class': final_opponent_class,
                    'first_second': first_second_val, 'result': result_val,
                    'finish_turn': int(finish_turn_val) if finish_turn_val is not None else None,
                    'memo': memo_val
                }
                new_df_row = pd.DataFrame([new_record_data], columns=COLUMNS)
                if save_data(new_df_row, SPREADSHEET_ID, WORKSHEET_NAME):
                    success_placeholder.success("戦績を記録しました！")
                    
                    if 'inp_memo' in st.session_state:
                        try:
                            st.session_state.pop('inp_memo', None)
                        except Exception as e_memo:
                            st.error(f"inp_memo の pop でエラー: {e_memo}") 
                    
                    keys_to_pop_for_new_entry = [
                        'inp_season_new', 'inp_environment_new', 'inp_format_new', 'inp_group_new',
                        'inp_my_deck_new', 'inp_my_deck_type_new',
                        'inp_opponent_deck_new', 'inp_opponent_deck_type_new'
                    ]
                    for key_to_pop in keys_to_pop_for_new_entry:
                        st.session_state.pop(key_to_pop, None) 
                    
                    st.rerun()
                else:
                    error_placeholder.error("データの保存に失敗しました。Google Sheetsへの接続を確認してください。")
    
    show_analysis_section(df.copy())
    st.header("戦績一覧")
    if df.empty:
        st.info("まだ戦績データがありません。")
    else:
        display_columns = ['timestamp', 'season', 'environment', 'format', 'group',
                        'my_deck', 'my_deck_type', 'my_class', 
                        'opponent_deck', 'opponent_deck_type', 'opponent_class', 
                        'first_second', 'result', 'finish_turn', 'memo']
        cols_to_display_actual = [col for col in display_columns if col in df.columns]
        df_display = df.copy()
        if 'timestamp' in df_display.columns:
            # Robustly parse timestamps for display and filtering
            parsed_series = parse_timestamp_series(df_display['timestamp'])

            # Fill missing parsed timestamps by scanning other columns in the same row
            missing_mask = parsed_series.isna()
            if missing_mask.any():
                for idx in parsed_series[missing_mask].index:
                    filled = False
                    for col in df_display.columns:
                        try:
                            cell = df_display.at[idx, col]
                        except Exception:
                            cell = None
                        sub = extract_date_substring(cell)
                        if sub:
                            try:
                                pv = pd.to_datetime(sub, errors='coerce', infer_datetime_format=True)
                            except Exception:
                                pv = pd.NaT
                            if not pd.isna(pv):
                                parsed_series.at[idx] = pv
                                filled = True
                                break
                    # end scanning columns

            # Create a date-only column for filtering (YYYY-MM-DD)
            df_display['date_for_filter'] = parsed_series.dt.date

            # Prepare a human-friendly display column. Prefer original raw string when present,
            # otherwise show parsed value formatted as 'YYYY/MM/DD HH:MM:SS' or 'YYYY/MM/DD' when time is midnight.
            def make_display(orig, parsed_val):
                try:
                    orig_str = '' if pd.isna(orig) else str(orig)
                except Exception:
                    orig_str = ''
                if orig_str:
                    return orig_str
                if pd.isna(parsed_val):
                    return None
                # format parsed_val
                try:
                    if parsed_val.time() == pd.Timestamp(parsed_val).replace(hour=0, minute=0, second=0).time():
                        return parsed_val.strftime('%Y/%m/%d')
                    else:
                        return parsed_val.strftime('%Y/%m/%d %H:%M:%S')
                except Exception:
                    return str(parsed_val)

            df_display['timestamp_display'] = [make_display(o, p) for o, p in zip(df_display['timestamp'], parsed_series)]

            # Sort by parsed datetime where available, keeping NaT at the end
            df_display_sorted = df_display.copy()
            df_display_sorted['_sort_ts'] = parsed_series
            df_display_sorted = pd.concat([df_display_sorted[df_display_sorted['_sort_ts'].notna()].sort_values(by='_sort_ts', ascending=False), df_display_sorted[df_display_sorted['_sort_ts'].isna()]]).reset_index(drop=True)
            df_display_sorted = df_display_sorted.drop(columns=['_sort_ts'])
            # Replace display column for UI
            if 'timestamp' in cols_to_display_actual:
                # show timestamp_display instead of raw timestamp
                cols_to_display_actual = ['timestamp_display' if c == 'timestamp' else c for c in cols_to_display_actual]
        else:
            df_display_sorted = df_display.reset_index(drop=True)
    st.dataframe(df_display_sorted[cols_to_display_actual])
    csv_export = df.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="戦績データをCSVでダウンロード", data=csv_export,
        file_name='game_records_download.csv', mime='text/csv',
    )

if __name__ == '__main__':
    main()