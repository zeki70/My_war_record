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
    'season', 'date', 'environment', 'format', 'my_deck', 'my_deck_type','my_class', 
    'opponent_deck', 'opponent_deck_type','opponent_class',   'first_second',
    'result', 'finish_turn', 'memo'
]
NEW_ENTRY_LABEL = "（新しい値を入力）"
SELECT_PLACEHOLDER = "--- 選択してください ---" # 分析用
ALL_TYPES_PLACEHOLDER = "全タイプ" # 分析用

# --- パスワード認証のための設定 ---
def get_app_password():
    if hasattr(st, 'secrets') and "app_credentials" in st.secrets and "password" in st.secrets["app_credentials"]:
        return st.secrets["app_credentials"]["password"]
    else:
        st.warning("アプリケーションパスワードがSecretsに設定されていません。ローカルテスト用に 'test_password' を使用します。デプロイ時には必ずSecretsを設定してください。")
        return "test_password"
CORRECT_PASSWORD = get_app_password()

# ★★★ ここにクッキーマネージャの初期化コードを配置 ★★★
# st.secrets から暗号化キーを取得 (事前にStreamlit CloudのSecretsに設定してください)
# 例: [app_credentials]
#      cookie_encryption_key = "あなたの生成した秘密のキー"
cookie_encryption_key = st.secrets.get("app_credentials", {}).get("cookie_encryption_key", "FALLBACK_KEY_CHANGE_THIS_NOW_123!") # Secretsにキーがない場合のフォールバック(非推奨)
if cookie_encryption_key == "FALLBACK_KEY_CHANGE_THIS_NOW_123!":
    st.warning("クッキー暗号化キーがデフォルトのフォールバック値です。必ずSecretsで 'cookie_encryption_key' を設定してください。")

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
# --- データ操作関数 ---
def load_data(spreadsheet_id, worksheet_name):
    client = get_gspread_client()
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを読み込めません。認証情報を確認してください。")
        empty_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col == 'date': empty_df[col] = pd.Series(dtype='datetime64[ns]')
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

        # COLUMNS に基づいて DataFrame を整形し、不足列は適切な型で追加
        # この処理は、get_as_dataframe がヘッダー行を正しく解釈した後に実行される
        temp_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col in df.columns:
                temp_df[col] = df[col]
            else: # dfに列が存在しない場合は、空のSeriesを適切な型で作成
                if col == 'date': temp_df[col] = pd.Series(dtype='datetime64[ns]')
                elif col == 'finish_turn': temp_df[col] = pd.Series(dtype='Int64')
                else: temp_df[col] = pd.Series(dtype='object')
        df = temp_df
        
        # 型変換
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if 'finish_turn' in df.columns:
            df['finish_turn'] = pd.to_numeric(df['finish_turn'], errors='coerce').astype('Int64')

        # 文字列として扱う列の処理 (my_class, opponent_class を含む)
        string_cols = ['my_deck_type', 'my_class', 'opponent_deck_type', 'opponent_class',
                       'my_deck', 'opponent_deck', 'season', 'memo',
                       'first_second', 'result', 'environment', 'format']
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
            if col == 'date': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"ワークシート '{worksheet_name}' がスプレッドシート (ID: {spreadsheet_id}) 内に見つかりません。")
        df = pd.DataFrame(columns=COLUMNS) # 空のDataFrameを返す
        for col in COLUMNS: # 型情報を付与
            if col == 'date': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    except Exception as e:
        st.error(f"Google Sheetsからのデータ読み込み中に予期せぬエラーが発生しました: {type(e).__name__}: {e}")
        df = pd.DataFrame(columns=COLUMNS) # 空のDataFrameを返す
        for col in COLUMNS: # 型情報を付与
            if col == 'date': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    return df
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
                if pd.isna(value): data_to_append.append("")
                elif col == 'date' and isinstance(value, (datetime, pd.Timestamp)):
                     data_to_append.append(value.strftime('%Y-%m-%d'))
                elif col == 'finish_turn' and pd.notna(value):
                     data_to_append.append(int(value))
                else: data_to_append.append(str(value))
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

# --- 入力フォーム用ヘルパー関数 (シーズン・クラス絞り込み対応) ---

# --- 入力フォーム用ヘルパー関数 (シーズン・クラス絞り込み対応、デッキ名候補拡張) ---

# --- 入力フォーム用ヘルパー関数 (シーズン・クラス絞り込み、デッキ名/型候補拡張) ---

# --- 入力フォーム用ヘルパー関数 (シーズン・クラス・フォーマット絞り込み対応) ---

def get_decks_for_filter_conditions_input(df, selected_season, selected_ui_class, selected_format):
    """
    指定されたシーズン、UIで選択されたクラス、選択されたフォーマットに基づいて、
    my_deck と opponent_deck の両方から該当するユニークなデッキ名のリストを取得する。
    selected_ui_class: UI上で選択されたクラス名
    selected_format: UI上で選択されたフォーマット名
    """
    if not selected_ui_class: # クラスが選択されていない場合は候補を絞り込めない
        return [NEW_ENTRY_LABEL]
    # フォーマットが選択されていない場合も候補を絞り込めない（または全フォーマットを対象とするか選べるが、今回は必須とする）
    if not selected_format or selected_format == NEW_ENTRY_LABEL: # NEW_ENTRY_LABEL は不正なフォーマット値として扱う
        return [NEW_ENTRY_LABEL]


    df_filtered = df.copy()

    # 1. シーズンで絞り込み
    if selected_season and selected_season != NEW_ENTRY_LABEL and pd.notna(selected_season):
        df_filtered = df_filtered[df_filtered['season'].astype(str) == str(selected_season)]
    
    # 2. フォーマットで絞り込み
    df_filtered = df_filtered[df_filtered['format'].astype(str) == str(selected_format)]
    
    if df_filtered.empty: # シーズンとフォーマットで絞り込んだ結果、データがなければ早期リターン
        return [NEW_ENTRY_LABEL]

    deck_names_set = set()

    # 3a. UIで選択されたクラスが「自分のクラス」列と一致する場合の「自分のデッキ名」を収集
    my_class_deck_df = df_filtered[df_filtered['my_class'].astype(str) == str(selected_ui_class)]
    if not my_class_deck_df.empty and 'my_deck' in my_class_deck_df.columns:
        valid_items_my_deck = my_class_deck_df['my_deck'].astype(str).replace('', pd.NA).dropna()
        deck_names_set.update(d for d in valid_items_my_deck.tolist() if d and d.lower() != 'nan')
    
    # 3b. UIで選択されたクラスが「相手のクラス」列と一致する場合の「相手のデッキ名」を収集
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
    if (not selected_ui_class or 
        not selected_deck_name or selected_deck_name == NEW_ENTRY_LABEL or pd.isna(selected_deck_name) or
        not selected_format or selected_format == NEW_ENTRY_LABEL): # フォーマットもチェック
        return [NEW_ENTRY_LABEL]

    df_filtered = df.copy()

    # 1. シーズンで絞り込み
    if selected_season and selected_season != NEW_ENTRY_LABEL and pd.notna(selected_season):
        df_filtered = df_filtered[df_filtered['season'].astype(str) == str(selected_season)]
    
    # 2. フォーマットで絞り込み
    df_filtered = df_filtered[df_filtered['format'].astype(str) == str(selected_format)]

    if df_filtered.empty: # シーズンとフォーマットで絞り込んだ結果、データがなければ早期リターン
        return [NEW_ENTRY_LABEL]

    types_set = set()

    # 3a. UIで選択されたクラスが「自分のクラス」で、かつ選択されたデッキ名が「自分のデッキ」の場合の「自分のデッキタイプ」を収集
    my_context_df = df_filtered[
        (df_filtered['my_class'].astype(str) == str(selected_ui_class)) &
        (df_filtered['my_deck'].astype(str) == str(selected_deck_name))
    ]
    if not my_context_df.empty and 'my_deck_type' in my_context_df.columns:
        valid_items_my_type = my_context_df['my_deck_type'].astype(str).replace('', pd.NA).dropna()
        types_set.update(t for t in valid_items_my_type.tolist() if t and t.lower() != 'nan')

    # 3b. UIで選択されたクラスが「相手のクラス」で、かつ選択されたデッキ名が「相手のデッキ」の場合の「相手のデッキタイプ」を収集
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
    """
    指定されたシーズン、UIで選択されたクラス、UIで選択されたデッキ名に基づいて、
    my_deck_type と opponent_deck_type の両方から該当するユニークなデッキタイプのリストを取得する。
    """
    if (not selected_ui_class or 
        not selected_deck_name or selected_deck_name == NEW_ENTRY_LABEL or pd.isna(selected_deck_name)):
        return [NEW_ENTRY_LABEL]

    df_filtered_by_season = df.copy()

    # 1. シーズンで絞り込み
    if selected_season and selected_season != NEW_ENTRY_LABEL and pd.notna(selected_season):
        df_filtered_by_season = df_filtered_by_season[df_filtered_by_season['season'].astype(str) == str(selected_season)]

    if df_filtered_by_season.empty:
        return [NEW_ENTRY_LABEL]

    types_set = set()

    # 2a. UIで選択されたクラスが「自分のクラス」で、かつ選択されたデッキ名が「自分のデッキ」の場合の「自分のデッキタイプ」を収集
    my_context_df = df_filtered_by_season[
        (df_filtered_by_season['my_class'].astype(str) == str(selected_ui_class)) &
        (df_filtered_by_season['my_deck'].astype(str) == str(selected_deck_name))
    ]
    if not my_context_df.empty and 'my_deck_type' in my_context_df.columns:
        valid_items_my_type = my_context_df['my_deck_type'].astype(str).replace('', pd.NA).dropna()
        types_set.update(t for t in valid_items_my_type.tolist() if t and t.lower() != 'nan')

    # 2b. UIで選択されたクラスが「相手のクラス」で、かつ選択されたデッキ名が「相手のデッキ」の場合の「相手のデッキタイプ」を収集
    opponent_context_df = df_filtered_by_season[
        (df_filtered_by_season['opponent_class'].astype(str) == str(selected_ui_class)) &
        (df_filtered_by_season['opponent_deck'].astype(str) == str(selected_deck_name))
    ]
    if not opponent_context_df.empty and 'opponent_deck_type' in opponent_context_df.columns:
        valid_items_opponent_type = opponent_context_df['opponent_deck_type'].astype(str).replace('', pd.NA).dropna()
        types_set.update(t for t in valid_items_opponent_type.tolist() if t and t.lower() != 'nan')

    if not types_set:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(types_set))
# --- 分析用ヘルパー関数 ---
def get_all_analyzable_deck_names(df):
    ### 変更点 ### 自分が使用したデッキ（my_deck）のみを分析対象とする
    my_decks = df['my_deck'].astype(str).replace('', pd.NA).dropna().unique()
    all_decks_set = set(my_decks) # opponent_decks の収集を削除
    return sorted([d for d in all_decks_set if d and d.lower() != 'nan'])
def get_all_types_for_archetype(df, deck_name):
    ### 変更点 ### 注目デッキが「自分のデッキ」として使われた際の「自分のデッキの型」のみを収集
    if not deck_name or deck_name == SELECT_PLACEHOLDER or pd.isna(deck_name):
        return [ALL_TYPES_PLACEHOLDER]
    types = set()
    # 注目デッキが「自分のデッキ」として使われた際の「自分のデッキの型」のみを収集
    my_deck_matches = df[(df['my_deck'].astype(str) == str(deck_name))]
    if not my_deck_matches.empty and 'my_deck_type' in my_deck_matches.columns:
        types.update(my_deck_matches['my_deck_type'].astype(str).replace('', pd.NA).dropna().tolist())
    # ### 削除 ### opponent_deck や opponent_deck_type からの収集は不要
    valid_types = sorted([t for t in list(types) if t and t.lower() != 'nan'])
    return [ALL_TYPES_PLACEHOLDER] + valid_types
# --- 分析セクション表示関数 ---
# --- 分析セクション表示関数 ---
def display_general_deck_performance(df_to_analyze):
    ### 変更点 ### 「自分の使用したデッキ」のパフォーマンス概要に変更
    st.subheader("使用デッキ パフォーマンス概要") # タイトル変更
    all_my_deck_archetypes = get_all_analyzable_deck_names(df_to_analyze) # 関数がmy_deckのみ返すように変更済み
    if not all_my_deck_archetypes:
        st.info("分析可能な使用デッキデータが現在の絞り込み条件ではありません。")
        return

    general_performance_data = []
    for deck_a_name in all_my_deck_archetypes:
        if not deck_a_name: continue

        # 自分がそのデッキを使用したゲームのみを対象
        games_as_my_deck_df = df_to_analyze[df_to_analyze['my_deck'] == deck_a_name]
        if games_as_my_deck_df.empty:
            continue

        wins_as_my_deck = len(games_as_my_deck_df[games_as_my_deck_df['result'] == '勝ち'])
        count_as_my_deck = len(games_as_my_deck_df)

        # ### 削除 ### opponent_deckとしての集計は不要

        total_appearances_deck_a = count_as_my_deck # 自分の使用回数
        total_wins_deck_a = wins_as_my_deck        # 自分が使用して勝った回数
        total_losses_deck_a = total_appearances_deck_a - total_wins_deck_a
        simple_overall_win_rate_deck_a = (total_wins_deck_a / total_appearances_deck_a * 100) if total_appearances_deck_a > 0 else 0.0

        # 先攻/後攻別勝率 (自分が使用した場合のみ)
        deck_a_first_as_my = games_as_my_deck_df[games_as_my_deck_df['first_second'] == '先攻']
        total_games_deck_a_first = len(deck_a_first_as_my)
        wins_deck_a_first = len(deck_a_first_as_my[deck_a_first_as_my['result'] == '勝ち'])
        win_rate_deck_a_first = (wins_deck_a_first / total_games_deck_a_first * 100) if total_games_deck_a_first > 0 else None

        deck_a_second_as_my = games_as_my_deck_df[games_as_my_deck_df['first_second'] == '後攻']
        total_games_deck_a_second = len(deck_a_second_as_my)
        wins_deck_a_second = len(deck_a_second_as_my[deck_a_second_as_my['result'] == '勝ち'])
        win_rate_deck_a_second = (wins_deck_a_second / total_games_deck_a_second * 100) if total_games_deck_a_second > 0 else None

        # 平均マッチアップ勝率 (自分がデッキAを使った際の、各対戦相手デッキに対する勝率の平均)
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
    st.subheader("対戦相手デッキ傾向分析") # 新しいセクションのタイトル

    if df_to_analyze.empty:
        st.info("分析対象のデータがありません。")
        return

    # 'opponent_deck' 列が存在しない、または全て欠損値の場合は処理をスキップ
    if 'opponent_deck' not in df_to_analyze.columns or df_to_analyze['opponent_deck'].dropna().empty:
        st.info("対戦相手のデッキ情報が記録されていません。")
        return

    # 相手デッキの出現回数を集計 (NaNや空文字列を除外)
    valid_opponent_decks = df_to_analyze['opponent_deck'].astype(str).replace('', pd.NA).dropna()
    if valid_opponent_decks.empty:
        st.info("集計可能な対戦相手のデッキ情報がありません。")
        return
        
    opponent_deck_counts = valid_opponent_decks.value_counts().reset_index()
    opponent_deck_counts.columns = ['対戦相手デッキ', '遭遇回数']
    
    total_games_in_scope = len(df_to_analyze) # 絞り込まれた範囲内の全ゲーム数

    summary_data = []
    for index, row in opponent_deck_counts.iterrows():
        opp_deck_name = row['対戦相手デッキ']
        appearances = row['遭遇回数']

        # value_counts が既に NaN や空文字列を除外しているはずだが、念のため
        if not opp_deck_name or str(opp_deck_name).lower() == 'nan' or str(opp_deck_name).strip() == "":
            continue

        games_vs_this_opp = df_to_analyze[df_to_analyze['opponent_deck'] == opp_deck_name]
        
        my_wins_vs_opp = len(games_vs_this_opp[games_vs_this_opp['result'] == '勝ち'])
        my_losses_vs_opp = appearances - my_wins_vs_opp
        
        win_rate_vs_opp = (my_wins_vs_opp / appearances * 100) if appearances > 0 else None
        usage_percentage = (appearances / total_games_in_scope * 100) if total_games_in_scope > 0 else 0

        # 平均決着ターン（対戦相手がこのデッキだった場合、決着ターン0は除外）
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
        st.info("集計可能な対戦相手のデッキ情報がありません。")
        return

    summary_df = pd.DataFrame(summary_data)
    # 遭遇回数が多い順、次に自分の勝率が高い順でソート（任意）
    summary_df = summary_df.sort_values(by=["遭遇回数", "自分の勝率 (%)"], ascending=[False, False]).reset_index(drop=True)

    display_cols = ["相手デッキ", "遭遇回数", "遭遇率 (%)", "自分勝利数", "自分敗北数", "自分の勝率 (%)", "平均決着ターン"]
    
    # 表示する列が存在するか確認
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

    # 先攻/後攻別パフォーマンス
    first_games_df = df_to_analyze[df_to_analyze['first_second'] == '先攻']
    total_first_games = len(first_games_df)
    wins_first = len(first_games_df[first_games_df['result'] == '勝ち'])
    win_rate_first = (wins_first / total_first_games * 100) if total_first_games > 0 else None

    second_games_df = df_to_analyze[df_to_analyze['first_second'] == '後攻']
    total_second_games = len(second_games_df)
    wins_second = len(second_games_df[second_games_df['result'] == '勝ち'])
    win_rate_second = (wins_second / total_second_games * 100) if total_second_games > 0 else None
    
    # --- ▼▼▼ 平均決着ターンの計算ロジックを削除しました ▼▼▼ ---
    # avg_win_finish_turn = None
    # (勝利時平均決着ターンの計算 ... )
    # avg_loss_finish_turn = None
    # (敗北時平均決着ターンの計算 ... )
    # --- ▲▲▲ 平均決着ターンの計算ロジック削除ここまで ▲▲▲ ---

    # st.metric を使って表示
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("総対戦数", f"{total_games} 戦")
        st.metric("先攻時勝率", 
                  f"{win_rate_first:.1f}%" if win_rate_first is not None else "N/A",
                  help=f"先攻 {total_first_games}戦 {wins_first}勝" if total_first_games > 0 else "データなし")
        # --- ▼▼▼ 「勝利時 平均決着T」の st.metric 表示を削除しました ▼▼▼ ---
        # st.metric("勝利時 平均決着T", ...) 
        # --- ▲▲▲ 表示削除ここまで ▲▲▲ ---
        
    with col2:
        st.metric("総勝利数", f"{total_wins} 勝")
        st.metric("後攻時勝率", 
                  f"{win_rate_second:.1f}%" if win_rate_second is not None else "N/A",
                  help=f"後攻 {total_second_games}戦 {wins_second}勝" if total_second_games > 0 else "データなし")
        # --- ▼▼▼ 「敗北時 平均決着T」の st.metric 表示を削除しました ▼▼▼ ---
        # st.metric("敗北時 平均決着T", ...)
        # --- ▲▲▲ 表示削除ここまで ▲▲▲ ---

    with col3:
        st.metric("総敗北数", f"{total_losses} 敗")
        st.metric("総合勝率", f"{overall_win_rate:.1f}%" if overall_win_rate is not None else "N/A")

def show_analysis_section(original_df):
    st.header("📊 戦績分析")
    if original_df.empty:
        st.info("まだ分析できる戦績データがありません。")
        return
    st.subheader("絞り込み条件")
    # --- ▼▼▼ 「フォーマットで絞り込み」の初期値を設定 ▼▼▼ ---
    # まず、利用可能なフォーマットの選択肢リストを作成（SELECT_PLACEHOLDERなし）
    available_formats_in_data = sorted([
        f for f in original_df['format'].astype(str).replace('', pd.NA).dropna().unique() 
        if f and f.lower() != 'nan'
    ])

    if 'ana_format_filter' not in st.session_state: # セッションステートにキーがまだ存在しない場合のみ初期値を設定
        if "ローテーション" in available_formats_in_data:
            st.session_state.ana_format_filter = ["ローテーション"]
        else:
            # "ローテーション" がデータ内に存在しない場合は、何も選択しない (空リスト)
            # または、もし他のデフォルト挙動が必要ならここで設定 (例: available_formats_in_data[0] など)
            st.session_state.ana_format_filter = [] 
    # --- ▲▲▲ 初期値設定ここまで ▲▲▲ ---

    all_seasons = [SELECT_PLACEHOLDER] + sorted([s for s in original_df['season'].astype(str).replace('', pd.NA).dropna().unique() if s and s.lower() != 'nan'])
    selected_season_for_analysis = st.selectbox("シーズンで絞り込み (任意):", options=all_seasons, key='ana_season_filter')

    all_environments = [SELECT_PLACEHOLDER] + sorted([e for e in original_df['environment'].astype(str).replace('', pd.NA).dropna().unique() if e and e.lower() != 'nan'])
    selected_environments = st.multiselect("対戦環境で絞り込み (任意):", options=all_environments, key='ana_environment_filter')

    all_formats = [SELECT_PLACEHOLDER] + sorted([f for f in original_df['format'].astype(str).replace('', pd.NA).dropna().unique() if f and f.lower() != 'nan'])
    selected_formats = st.multiselect("フォーマットで絞り込み (任意):", options=all_formats, key='ana_format_filter')

    df_for_analysis = original_df.copy()
    if selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER:
        df_for_analysis = df_for_analysis[df_for_analysis['season'] == selected_season_for_analysis]
    if selected_environments:
        df_for_analysis = df_for_analysis[df_for_analysis['environment'].isin(selected_environments)]
    if selected_formats:
        df_for_analysis = df_for_analysis[df_for_analysis['format'].isin(selected_formats)]

    if df_for_analysis.empty:
        if (selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER) or selected_environments or selected_formats:
            st.warning("選択された絞り込み条件に合致するデータがありません。")
        else: st.info("分析対象のデータがありません。")
        return

    st.subheader("使用デッキ詳細分析") # タイトル変更
    def reset_focus_type_callback():
        st.session_state.ana_focus_deck_type_selector = ALL_TYPES_PLACEHOLDER
        if 'inp_ana_focus_deck_type_new' in st.session_state:
            st.session_state.inp_ana_focus_deck_type_new = ""

    deck_names_for_focus_options = [SELECT_PLACEHOLDER] + get_all_analyzable_deck_names(df_for_analysis)
    st.selectbox("分析する使用デッキアーキタイプを選択:", options=deck_names_for_focus_options, key='ana_focus_deck_name_selector', on_change=reset_focus_type_callback) # 文言変更
    selected_focus_deck = st.session_state.get('ana_focus_deck_name_selector')

    if selected_focus_deck and selected_focus_deck != SELECT_PLACEHOLDER:
        types_for_focus_deck_options = get_all_types_for_archetype(df_for_analysis, selected_focus_deck)
        st.selectbox("使用デッキの型を選択 (「全タイプ」で型を問わず集計):", options=types_for_focus_deck_options, key='ana_focus_deck_type_selector') # 文言変更
        selected_focus_type = st.session_state.get('ana_focus_deck_type_selector')
        st.markdown("---")
        focus_deck_display_name = f"{selected_focus_deck}"
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            focus_deck_display_name += f" ({selected_focus_type})"
        st.subheader(f"「{focus_deck_display_name}」使用時の分析結果") # タイトル変更

        cond_my_deck_focus = (df_for_analysis['my_deck'] == selected_focus_deck)
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            cond_my_deck_focus &= (df_for_analysis['my_deck_type'] == selected_focus_type)
        focus_as_my_deck_games = df_for_analysis[cond_my_deck_focus]

        # ### 削除 ### opponent_deck が focus_deck である場合の考慮は不要

        total_appearances = len(focus_as_my_deck_games)
        if total_appearances == 0:
            st.warning(f"「{focus_deck_display_name}」の使用記録が現在の絞り込み条件で見つかりません。") # 文言変更
            return

        wins_when_focus_is_my_deck_df = focus_as_my_deck_games[focus_as_my_deck_games['result'] == '勝ち']
        total_wins_for_focus_deck = len(wins_when_focus_is_my_deck_df)

        win_rate_for_focus_deck = (total_wins_for_focus_deck / total_appearances * 100) if total_appearances > 0 else 0.0

        win_finish_turns = []
        # ▼▼▼ 修正箇所 ▼▼▼
        if not wins_when_focus_is_my_deck_df.empty and 'finish_turn' in wins_when_focus_is_my_deck_df.columns:
            valid_turns = wins_when_focus_is_my_deck_df['finish_turn'].dropna().astype(float) # astype(float) を追加して比較できるようにする
            win_finish_turns.extend(valid_turns[valid_turns > 0].tolist())
        # ▲▲▲ 修正ここまで ▲▲▲
        avg_win_finish_turn_val = pd.Series(win_finish_turns).mean() if win_finish_turns else None

        focus_first_my = focus_as_my_deck_games[focus_as_my_deck_games['first_second'] == '先攻']
        total_games_focus_first = len(focus_first_my)
        wins_focus_first = len(focus_first_my[focus_first_my['result'] == '勝ち'])
        win_rate_focus_first = (wins_focus_first / total_games_focus_first * 100) if total_games_focus_first > 0 else None

        focus_second_my = focus_as_my_deck_games[focus_as_my_deck_games['first_second'] == '後攻']
        total_games_focus_second = len(focus_second_my)
        wins_focus_second = len(focus_second_my[focus_second_my['result'] == '勝ち'])
        win_rate_focus_second = (wins_focus_second / total_games_focus_second * 100) if total_games_focus_second > 0 else None

        st.markdown("**総合パフォーマンス (使用者視点)**") # 文言変更
        perf_col1, perf_col2, perf_col3 = st.columns(3)
        with perf_col1:
            st.metric("総使用回数", total_appearances) # 文言変更
            st.metric("先攻時勝率", f"{win_rate_focus_first:.1f}%" if win_rate_focus_first is not None else "N/A",
                      help=f"先攻時 {wins_focus_first}勝 / {total_games_focus_first}戦" if total_games_focus_first > 0 else "データなし")
        with perf_col2:
            st.metric("総勝利数", total_wins_for_focus_deck)
            st.metric("後攻時勝率", f"{win_rate_focus_second:.1f}%" if win_rate_focus_second is not None else "N/A",
                      help=f"後攻時 {wins_focus_second}勝 / {total_games_focus_second}戦" if total_games_focus_second > 0 else "データなし")
        with perf_col3:
            st.metric("総合勝率", f"{win_rate_for_focus_deck:.1f}%")
            st.metric("勝利時平均ターン", f"{avg_win_finish_turn_val:.1f} T" if avg_win_finish_turn_val is not None else "N/A")

 ### 追加部分ここから ###
        st.markdown("---")
        st.subheader(f"「{focus_deck_display_name}」使用時の対戦相手傾向")

        if not focus_as_my_deck_games.empty:
            # 相手デッキアーキタイプ別の登場回数と、それに対する勝敗・勝率を計算
            opponent_deck_summary_list = []
            # total_appearances は注目デッキの総使用回数 (既に計算済み)

            # まずは相手デッキアーキタイプごとに集計
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
                    column_config={ # 列名やヘルプテキストをカスタマイズ（任意）
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
        ### 追加部分ここまで ###
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
            focus_deck_win_turns_vs_opp.extend(case1_wins_df['finish_turn'].dropna().tolist())
            # ▼▼▼ 修正箇所 ▼▼▼
            if not case1_wins_df.empty and 'finish_turn' in case1_wins_df.columns:
                valid_win_turns = case1_wins_df['finish_turn'].dropna().astype(float)
                focus_deck_win_turns_vs_opp.extend(valid_win_turns[valid_win_turns > 0].tolist())
            # ▲▲▲ 修正ここまで ▲▲▲
            focus_deck_loss_turns_vs_opp.extend(case1_losses_df['finish_turn'].dropna().tolist())
            # ▼▼▼ 修正箇所 ▼▼▼
            if not case1_losses_df.empty and 'finish_turn' in case1_losses_df.columns:
                valid_loss_turns = case1_losses_df['finish_turn'].dropna().astype(float)
                focus_deck_loss_turns_vs_opp.extend(valid_loss_turns[valid_loss_turns > 0].tolist())
            # ▲▲▲ 修正ここまで ▲▲▲

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
                games_played_display = f"{games_played_count} (自分の先攻: {fd_vs_opp_first_games_count})" # 文言変更

                matchup_data.append({
                    "対戦相手デッキ": opp_deck_name, "対戦相手デッキの型": opp_deck_type,
                    "対戦数": games_played_display, "(自分の)勝利数": focus_deck_wins_count, # 文言変更
                    "(自分の)勝率(%)": win_rate_vs_opp, # 文言変更
                    "勝利時平均ターン": avg_win_turn, "敗北時平均ターン": avg_loss_turn,
                    "(自分の)先攻時勝率(%)": win_rate_fd_first_vs_opp, "(自分の)後攻時勝率(%)": win_rate_fd_second_vs_opp # 文言変更
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
                all_win_turns_agg = focus_wins_agg1_df['finish_turn'].dropna().tolist()
                # ▼▼▼ 修正箇所 ▼▼▼
                if not focus_wins_agg1_df.empty and 'finish_turn' in focus_wins_agg1_df.columns:
                    valid_all_win_turns = focus_wins_agg1_df['finish_turn'].dropna().astype(float)
                    all_win_turns_agg.extend(valid_all_win_turns[valid_all_win_turns > 0].tolist())
                # ▲▲▲ 修正ここまで ▲▲▲
                all_loss_turns_agg = focus_losses_agg1_df['finish_turn'].dropna().tolist()
                # ▼▼▼ 修正箇所 ▼▼▼
                if not focus_losses_agg1_df.empty and 'finish_turn' in focus_losses_agg1_df.columns:
                    valid_all_loss_turns = focus_losses_agg1_df['finish_turn'].dropna().astype(float)
                    all_loss_turns_agg.extend(valid_all_loss_turns[valid_all_loss_turns > 0].tolist())
                # ▲▲▲ 修正ここまで ▲▲▲

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

                games_played_display_agg = f"{total_games_vs_opp_deck_agg} (自分の先攻: {fd_first_games_agg_total_count})" # 文言変更
                if total_games_vs_opp_deck_agg > 0:
                    agg_matchup_data.append({
                        "対戦相手デッキ": opp_deck_name_agg, "対戦相手デッキの型": ALL_TYPES_PLACEHOLDER,
                        "対戦数": games_played_display_agg, "(自分の)勝利数": total_focus_wins_vs_opp_deck_agg, # 文言変更
                        "(自分の)勝率(%)": win_rate_vs_opp_deck_agg, # 文言変更
                        "勝利時平均ターン": avg_win_turn_agg, "敗北時平均ターン": avg_loss_turn_agg,
                        "(自分の)先攻時勝率(%)": win_rate_fd_first_agg_total, "(自分の)後攻時勝率(%)": win_rate_fd_second_agg_total # 文言変更
                    })
            matchup_df_all_types = pd.DataFrame(agg_matchup_data)
            matchup_df_combined = pd.concat([matchup_df_specific_types, matchup_df_all_types], ignore_index=True)
            if not matchup_df_combined.empty:
                matchup_df_combined['__sort_type'] = matchup_df_combined['対戦相手デッキの型'].apply(lambda x: ('0_AllTypes' if x == ALL_TYPES_PLACEHOLDER else '1_' + str(x)))
                matchup_df_final = matchup_df_combined.sort_values(by=["対戦相手デッキ", "__sort_type"]).drop(columns=['__sort_type']).reset_index(drop=True)
                st.dataframe(matchup_df_final.style.format({
                    "(自分の)勝率(%)": "{:.1f}%", # 文言変更
                    "勝利時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "敗北時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "(自分の)先攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A", # 文言変更
                    "(自分の)後攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A" # 文言変更
                }), use_container_width=True)
            else: st.info(f"「{focus_deck_display_name}」使用時の対戦相手別の記録が見つかりません。") # 文言変更
        else: st.info(f"「{focus_deck_display_name}」使用時の対戦相手別の記録が見つかりません。") # 文言変更

        st.markdown("---")
        st.subheader(f"📝 「{focus_deck_display_name}」使用時のメモ付き対戦記録") # 文言変更
        memo_filter_my_deck = (focus_as_my_deck_games['memo'].astype(str).str.strip() != '') & \
                              (focus_as_my_deck_games['memo'].astype(str).str.lower() != 'nan')
        memos_when_my_deck = focus_as_my_deck_games[memo_filter_my_deck]
        all_memo_games = memos_when_my_deck.reset_index(drop=True)

        if not all_memo_games.empty:
            memo_display_cols = ['date', 'season', 'environment', 'format', 'my_deck', 'my_deck_type', 'opponent_deck', 'opponent_deck_type', 'first_second', 'result', 'finish_turn', 'memo']
            actual_memo_display_cols = [col for col in memo_display_cols if col in all_memo_games.columns]
            df_memo_display = all_memo_games[actual_memo_display_cols].copy()
            if 'date' in df_memo_display.columns:
                df_memo_display['date'] = pd.to_datetime(df_memo_display['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            st.dataframe(df_memo_display.sort_values(by='date', ascending=False), use_container_width=True)
        else: st.info(f"「{focus_deck_display_name}」使用時のメモ付きの記録は、現在の絞り込み条件ではありません。") # 文言変更
    else:
        display_overall_filtered_performance(df_for_analysis)
        display_general_deck_performance(df_for_analysis)

            # display_general_deck_performance(df_for_analysis) # ★変更前：自分の使用デッキ概要を表示
        display_opponent_deck_summary(df_for_analysis)   # ★変更後：相手デッキ傾向分析を表示
# --- Streamlit アプリ本体 (main関数) ---
def main():
    PREDEFINED_CLASSES = ["エルフ", "ロイヤル", "ウィッチ", "ドラゴン", "ナイトメア", "ビショップ", "ネメシス"] # 「ナイトメア」を「ネクロマンサー」に統一（またはお好みに合わせて調整）

    st.title(f"{SPREADSHEET_NAME_DISPLAY}") # タイトル表示をSPREADSHEET_NAME_DISPLAYに連動
    # st.title("Shadowverse戦績管理") # またはこのように直接指定も可能

    if SPREADSHEET_ID == "ここに実際の Waic-戦績 のスプレッドシートIDを貼り付け": # この警告は元のまま
        st.error("コード内の SPREADSHEET_ID を、お使いのGoogleスプレッドシートの実際のIDに置き換えてください。")
        st.warning("スプレッドシートIDは、スプレッドシートのURLに含まれる長い英数字の文字列です。")
        st.code("https://docs.google.com/spreadsheets/d/【この部分がIDです】/edit")
        st.stop()

    # --- ▼▼▼ 認証処理の変更 ▼▼▼ ---
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    # ★追加：アプリ起動時にクッキーを確認し、自動ログインを試みる
    if not st.session_state.authenticated: # まだst.session_stateで認証されていなければ
        try:
            stored_password_from_cookie = cookies.get('auth_password') # クッキーから保存されたパスワードを取得
            if stored_password_from_cookie and stored_password_from_cookie == CORRECT_PASSWORD:
                st.session_state.authenticated = True
                # 自動ログイン成功時は st.rerun() を呼ばない方がスムーズな場合がある
                # st.rerun() # 必要に応じて呼び出す
        except Exception as e:
            # クッキーのデコードエラーやその他の問題が発生した場合のフォールバック
            st.warning(f"クッキーの読み取り中にエラーが発生しました: {e}")
            pass # ログインフォームに進む

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
                        # ★追加：ログイン成功時にパスワードをクッキーに保存
                        cookies['auth_password'] = CORRECT_PASSWORD
                        # クッキーの有効期限を設定（例: 365日）
                        # cookies.set('auth_password', CORRECT_PASSWORD, expires_at=datetime.now() + timedelta(days=365))
                        # ↑ timedelta を使う場合は from datetime import timedelta が必要
                        # EncryptedCookieManager では set 時に expires_at を直接は指定できないようです。
                        # CookieManager の save メソッドでグローバルな有効期限を設定するか、
                        # ライブラリのドキュメントで詳細な有効期限設定方法を確認する必要があります。
                        # ここでは、ライブラリのデフォルトの有効期限（またはブラウザセッション）に依存します。
                        # より長期間の保持のためには、CookieManager の設定を調べるか、
                        # 単純にキーが存在し、CORRECT_PASSWORDと一致するかどうかで判断します。
                        # (EncryptedCookieManagerのデフォルトでは永続的なクッキーになることが多いです)
                        cookies.save() # 変更をクッキーに保存
                        st.rerun()
                    else:
                        st.error("パスワードが正しくありません。")
        st.stop()
    # --- ▲▲▲ 認証処理の変更ここまで ▲▲▲ ---
        st.stop()

    df = load_data(SPREADSHEET_ID, WORKSHEET_NAME)

    # --- ▼▼▼ アプリ初回起動時（または新セッション時）にスプレッドシートの最終行から入力値を読み込む ▼▼▼ ---
    if not st.session_state.get('form_values_initialized_from_gsheet', False):
        if not df.empty:
            last_entry = df.iloc[-1].copy() # 最終行を取得

            # セッションステートキーとDataFrameの列名のマッピング
            fields_to_load_from_gsheet = {
                'inp_season_select': 'season',
                'inp_date': 'date', # load_dataでdatetimeオブジェクトに変換済みのはず
                'inp_environment_select': 'environment',
                'inp_format_select': 'format',
                'inp_my_class': 'my_class',
                'inp_my_deck': 'my_deck',
                'inp_my_deck_type': 'my_deck_type',
                'inp_opponent_class': 'opponent_class',
                'inp_opponent_deck': 'opponent_deck',
                'inp_opponent_deck_type': 'opponent_deck_type',
                'inp_first_second': 'first_second',
                'inp_result': 'result',
                'inp_finish_turn': 'finish_turn' # load_dataでInt64 (nullable int) に変換済みのはず
                # 'inp_memo' は意図的に含めない
            }

            for session_key, df_col_name in fields_to_load_from_gsheet.items():
                if df_col_name in last_entry and pd.notna(last_entry[df_col_name]):
                    value_from_sheet = last_entry[df_col_name]
                    
                    if session_key == 'inp_date':
                        # df['date'] は load_data で pd.to_datetime されているので datetimeのはず
                        if isinstance(value_from_sheet, datetime):
                            st.session_state[session_key] = value_from_sheet.date()
                        elif isinstance(value_from_sheet, pd.Timestamp):
                             st.session_state[session_key] = value_from_sheet.date()
                        # 文字列からの変換は load_data で行われている前提
                    elif session_key == 'inp_finish_turn':
                        # df['finish_turn'] は load_data で Int64 (pd.NA を含むことがある)
                        if pd.notna(value_from_sheet): # pd.NA でないことを確認
                            try:
                                st.session_state[session_key] = int(value_from_sheet)
                            except (ValueError, TypeError):
                                # 変換に失敗した場合は、ウィジェットのデフォルト値に任せるため何もしない
                                pass
                        # pd.NA の場合も何もしない (ウィジェットのデフォルト値が使われる)
                    else:
                        # selectbox や text_input に渡す値は文字列が良い場合が多い
                        st.session_state[session_key] = str(value_from_sheet)
                # else: スプレッドシートの最終行に値がない場合は、st.session_state を設定せず、
                #       各ウィジェット定義時の st.session_state.get(key, default_value) の
                #       default_value が使われるようにする。
        
        st.session_state.form_values_initialized_from_gsheet = True # このセッションでは一度実行したフラグ
    # --- ▲▲▲ スプレッドシート最終行からの読み込み処理ここまで ▲▲▲ ---

# main() 関数内で定義

    # --- on_change コールバック関数の定義 ---
    def on_season_select_change_input_form():
        # シーズン変更時は、クラス選択は保持し、デッキ名とデッキタイプをリセット
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
        # 自分のクラス変更時は、自分のデッキ名とデッキタイプをリセット
        if 'inp_my_deck' in st.session_state: st.session_state.inp_my_deck = NEW_ENTRY_LABEL
        if 'inp_my_deck_new' in st.session_state: st.session_state.inp_my_deck_new = ""
        if 'inp_my_deck_type' in st.session_state: st.session_state.inp_my_deck_type = NEW_ENTRY_LABEL
        if 'inp_my_deck_type_new' in st.session_state: st.session_state.inp_my_deck_type_new = ""
        
    def on_opponent_class_select_change_input_form():
        # 相手のクラス変更時は、相手のデッキ名とデッキタイプをリセット
        if 'inp_opponent_deck' in st.session_state: st.session_state.inp_opponent_deck = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_new' in st.session_state: st.session_state.inp_opponent_deck_new = ""
        if 'inp_opponent_deck_type' in st.session_state: st.session_state.inp_opponent_deck_type = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_type_new' in st.session_state: st.session_state.inp_opponent_deck_type_new = ""

    def on_my_deck_select_change_input_form(): # 既存だが、呼び出し条件や中身が影響を受ける可能性
        # 自分のデッキ名変更時は、自分のデッキタイプをリセット
        if 'inp_my_deck_type' in st.session_state: st.session_state.inp_my_deck_type = NEW_ENTRY_LABEL
        if 'inp_my_deck_type_new' in st.session_state: st.session_state.inp_my_deck_type_new = ""

    def on_opponent_deck_select_change_input_form(): # 既存だが、呼び出し条件や中身が影響を受ける可能性
        # 相手のデッキ名変更時は、相手のデッキタイプをリセット
        if 'inp_opponent_deck_type' in st.session_state: st.session_state.inp_opponent_deck_type = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_type_new' in st.session_state: st.session_state.inp_opponent_deck_type_new = ""

    def on_format_select_change_input_form():
        # フォーマット変更時は、デッキ名とデッキタイプをリセット (自分と相手の両方)
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
    # --- コールバック定義ここまで ---
# main() 関数内の入力フォーム部分 (with st.expander(...) の中)

    with st.expander("戦績を入力する", expanded=True):
        st.subheader("対戦情報")
        # ... (シーズン、日付、環境、フォーマットの入力は変更なし、ただしシーズン選択のon_changeは上記で修正) ...
        season_options_input = get_unique_items_with_new_option(df, 'season')
        st.selectbox("シーズン *", season_options_input, key='inp_season_select',
                     help="例: 2025前期, 〇〇カップ", on_change=on_season_select_change_input_form) # on_change修正
        if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいシーズン名を入力 *", value=st.session_state.get('inp_season_new', ""), key='inp_season_new')
        
        default_dt_for_input = datetime.today().date()
        inp_date_value = st.session_state.get('inp_date', default_dt_for_input)
        # ... (日付入力のロジックはそのまま) ...
        st.date_input("対戦日", value=inp_date_value, key='inp_date')

        predefined_environments = ["ランクマッチ", "レート", "壁打ち"]
        # ... (対戦環境の入力ウィジェットはそのまま) ...
        environment_options_input = get_unique_items_with_new_option(df, 'environment', predefined_options=predefined_environments)
        st.selectbox("対戦環境 *", environment_options_input, key='inp_environment_select')
        if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL:
            st.text_input("新しい対戦環境を入力 *", value=st.session_state.get('inp_environment_new', ""), key='inp_environment_new')

        # st.write("---") # 区切りは元の形式に合わせて調整
        predefined_formats = ["ローテーション", "アンリミテッド", "2Pick"]
        # ... (フォーマットの入力ウィジェットはそのまま) ...
        format_options_input = get_unique_items_with_new_option(df, 'format', predefined_options=predefined_formats)
        st.selectbox("フォーマット *", format_options_input, key='inp_format_select', 
                     on_change=on_format_select_change_input_form) 
        if st.session_state.get('inp_format_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいフォーマット名を入力 *", value=st.session_state.get('inp_format_new', ""), key='inp_format_new')

        # 現在選択されているシーズンとクラスを後の処理で使うために取得
        # ▼▼▼ この部分で必要な変数を定義します ▼▼▼
        current_selected_season_input = st.session_state.get('inp_season_select')
        current_selected_format_input = st.session_state.get('inp_format_select') # ★この行が重要です★
        # ▼▼▼ 選択されたフォーマットを取得し、「2Pick」かどうかを判断 ▼▼▼
        current_selected_format_value = st.session_state.get('inp_format_select')
        if current_selected_format_value == NEW_ENTRY_LABEL: # 新規入力の場合も考慮
            current_selected_format_value = st.session_state.get('inp_format_new', '')
        
        is_2pick_format = (current_selected_format_value == "2Pick")
        # ▲▲▲ ここまで追加 ▲▲▲

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("自分のデッキ")
            
            # 1. 自分のクラスを選択
            st.selectbox("自分のクラス *", PREDEFINED_CLASSES, key='inp_my_class',
                         index=PREDEFINED_CLASSES.index(st.session_state.inp_my_class) if 'inp_my_class' in st.session_state and st.session_state.inp_my_class in PREDEFINED_CLASSES else 0,
                         on_change=on_my_class_select_change_input_form) # on_change追加
            current_my_class_input = st.session_state.get('inp_my_class')

              # ▼▼▼ 「2Pick」の場合、デッキ名と型選択をdisabledにする ▼▼▼
            my_deck_name_options_input = get_decks_for_filter_conditions_input(df, current_selected_season_input, current_my_class_input, current_selected_format_value)
            st.selectbox("使用デッキ *", my_deck_name_options_input, key='inp_my_deck', 
                         on_change=on_my_deck_select_change_input_form, 
                         disabled=is_2pick_format) # disabled追加
            if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL and not is_2pick_format: # 表示条件追加
                st.text_input("新しい使用デッキ名を入力 *", value=st.session_state.get('inp_my_deck_new', ""), key='inp_my_deck_new', disabled=is_2pick_format) # disabled追加
            current_my_deck_name_input = st.session_state.get('inp_my_deck')

            my_deck_type_options_input = get_types_for_filter_conditions_input(df, current_selected_season_input, current_my_class_input, current_my_deck_name_input, current_selected_format_value)
            st.selectbox("使用デッキの型 *", my_deck_type_options_input, key='inp_my_deck_type', 
                         disabled=is_2pick_format) # disabled追加
            if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL and not is_2pick_format: # 表示条件追加
                st.text_input("新しい使用デッキの型を入力 *", value=st.session_state.get('inp_my_deck_type_new', ""), key='inp_my_deck_type_new', disabled=is_2pick_format) # disabled追加
            # ▲▲▲ 修正ここまで ▲▲▲

        with col2:
            st.subheader("対戦相手のデッキ")

            # 1. 相手のクラスを選択
            st.selectbox("相手のクラス *", PREDEFINED_CLASSES, key='inp_opponent_class',
                         index=PREDEFINED_CLASSES.index(st.session_state.inp_opponent_class) if 'inp_opponent_class' in st.session_state and st.session_state.inp_opponent_class in PREDEFINED_CLASSES else 0,
                         on_change=on_opponent_class_select_change_input_form) # on_change追加
            current_opponent_class_input = st.session_state.get('inp_opponent_class')
            
            # ▼▼▼ 「2Pick」の場合、デッキ名と型選択をdisabledにする ▼▼▼
            opponent_deck_name_options_input = get_decks_for_filter_conditions_input(df, current_selected_season_input, current_opponent_class_input, current_selected_format_value)
            st.selectbox("相手デッキ *", opponent_deck_name_options_input, key='inp_opponent_deck', 
                         on_change=on_opponent_deck_select_change_input_form, 
                         disabled=is_2pick_format) # disabled追加
            if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL and not is_2pick_format: # 表示条件追加
                st.text_input("新しい相手デッキ名を入力 *", value=st.session_state.get('inp_opponent_deck_new', ""), key='inp_opponent_deck_new', disabled=is_2pick_format) # disabled追加
            current_opponent_deck_name_input = st.session_state.get('inp_opponent_deck')

            opponent_deck_type_options_input = get_types_for_filter_conditions_input(df, current_selected_season_input, current_opponent_class_input, current_opponent_deck_name_input, current_selected_format_value)
            st.selectbox("相手デッキの型 *", opponent_deck_type_options_input, key='inp_opponent_deck_type', 
                         disabled=is_2pick_format) # disabled追加
            if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL and not is_2pick_format: # 表示条件追加
                st.text_input("新しい相手デッキの型を入力 *", value=st.session_state.get('inp_opponent_deck_type_new', ""), key='inp_opponent_deck_type_new', disabled=is_2pick_format) # disabled追加
            # ▲▲▲ 修正ここまで ▲▲▲
        
        # ... (対戦結果、メモ、記録ボタン、エラー/成功メッセージ表示のロジックは変更なし) ...

        st.subheader("対戦結果")
        # res_col1, res_col2, res_col3 を使うか、縦に並べるかはお好みで。以前の形式に戻すなら列を使う。
        res_col1, res_col2, res_col3 = st.columns(3)
        with res_col1:
            st.selectbox("自分の先攻/後攻 *", ["先攻", "後攻"], key='inp_first_second', index=0 if 'inp_first_second' not in st.session_state else ["先攻", "後攻"].index(st.session_state.inp_first_second))
        with res_col2:
            st.selectbox("勝敗 *", ["勝ち", "負け"], key='inp_result', index=0 if 'inp_result' not in st.session_state else ["勝ち", "負け"].index(st.session_state.inp_result))
        with res_col3:
            st.number_input("決着ターン *", min_value=0, step=1, value=st.session_state.get('inp_finish_turn', 7), placeholder="ターン数を入力", key='inp_finish_turn',help="0はリタイア") # デフォルト値は適宜変更
        
        st.text_area("対戦メモ (任意)", value=st.session_state.get('inp_memo', ""), key='inp_memo')

        st.markdown("---")
        error_placeholder = st.empty()
        success_placeholder = st.empty()

        if st.button("戦績を記録", key='submit_record_button'):
            # ▼▼▼ ここから不足している可能性のある変数の定義を追加・確認 ▼▼▼
            final_season = st.session_state.get('inp_season_new', '') if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_season_select')
            # NEW_ENTRY_LABEL のまま残らないようにする処理も追加 (シーズン以外も同様)
            if final_season == NEW_ENTRY_LABEL: final_season = ''


            final_my_deck = st.session_state.get('inp_my_deck_new', '') if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck')
            if final_my_deck == NEW_ENTRY_LABEL: final_my_deck = ''

            final_my_deck_type = st.session_state.get('inp_my_deck_type_new', '') if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck_type')
            if final_my_deck_type == NEW_ENTRY_LABEL: final_my_deck_type = ''

            final_opponent_deck = st.session_state.get('inp_opponent_deck_new', '') if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck')
            if final_opponent_deck == NEW_ENTRY_LABEL: final_opponent_deck = ''

            final_opponent_deck_type = st.session_state.get('inp_opponent_deck_type_new', '') if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck_type')
            if final_opponent_deck_type == NEW_ENTRY_LABEL: final_opponent_deck_type = ''

            final_environment = st.session_state.get('inp_environment_new', '') if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_environment_select')
            if final_environment == NEW_ENTRY_LABEL : final_environment = ''

            final_format = st.session_state.get('inp_format_new', '') if st.session_state.get('inp_format_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_format_select')
            if final_format == NEW_ENTRY_LABEL: final_format = ''

            # ▼▼▼ 「2Pick」かどうかの判定（記録時）▼▼▼
            is_2pick_submit_time = (final_format == "2Pick")
            # ▲▲▲ ここまで追加 ▲▲▲

            if is_2pick_submit_time:
                final_my_deck = "2Pickデッキ"  # 固定値
                final_my_deck_type = "2Pick" # 固定値 (または空欄 "")
                final_opponent_deck = "2Pickデッキ" # 固定値
                final_opponent_deck_type = "2Pick"  # 固定値 (または空欄 "")
            else:
                final_my_deck = st.session_state.get('inp_my_deck_new', '') if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck')
                if final_my_deck == NEW_ENTRY_LABEL: final_my_deck = ''
                final_my_deck_type = st.session_state.get('inp_my_deck_type_new', '') if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck_type')
                if final_my_deck_type == NEW_ENTRY_LABEL: final_my_deck_type = ''
                final_opponent_deck = st.session_state.get('inp_opponent_deck_new', '') if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck')
                if final_opponent_deck == NEW_ENTRY_LABEL: final_opponent_deck = ''
                final_opponent_deck_type = st.session_state.get('inp_opponent_deck_type_new', '') if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck_type')
                if final_opponent_deck_type == NEW_ENTRY_LABEL: final_opponent_deck_type = ''


            # クラス情報の取得 (これは前回修正したものです)
            final_my_class = st.session_state.get('inp_my_class')
            final_opponent_class = st.session_state.get('inp_opponent_class')

            # 日付、先攻/後攻、結果などの取得
            date_val_from_state = st.session_state.get('inp_date')
            if isinstance(date_val_from_state, datetime): date_val = date_val_from_state.date()
            elif isinstance(date_val_from_state, type(datetime.today().date())): date_val = date_val_from_state
            else:
                try: date_val = pd.to_datetime(date_val_from_state).date()
                except: date_val = datetime.today().date() # エラー時は今日の日付

            first_second_val = st.session_state.get('inp_first_second')
            result_val = st.session_state.get('inp_result')
            finish_turn_val = st.session_state.get('inp_finish_turn')
            memo_val = st.session_state.get('inp_memo', '')
            # ▲▲▲ ここまで変数の定義 ▲▲▲

            error_messages = []
            if not final_season: error_messages.append("シーズンを入力または選択してください。")
            if not final_environment: error_messages.append("対戦環境を選択または入力してください。")
            if not final_format: error_messages.append("フォーマットを選択または入力してください。")
            
            if not final_my_class: error_messages.append("自分のクラスを選択してください。")
            if not final_opponent_class: error_messages.append("相手のクラスを選択してください。")

            # ▼▼▼ デッキ名・型の必須チェックを「2Pick」以外の場合のみに限定 ▼▼▼
            if not is_2pick_submit_time:
                if not final_my_deck: error_messages.append("使用デッキ名を入力または選択してください。")
                if not final_my_deck_type: error_messages.append("使用デッキの型を入力または選択してください。")
                if not final_opponent_deck: error_messages.append("相手デッキ名を入力または選択してください。")
                if not final_opponent_deck_type: error_messages.append("相手デッキの型を入力または選択してください。")
            # ▲▲▲ 修正ここまで ▲▲▲
            
            if finish_turn_val is None: error_messages.append("決着ターンを入力してください。")

            if error_messages:
                error_placeholder.error("、".join(error_messages))
                success_placeholder.empty()
            else:
                error_placeholder.empty()
                new_record_data = {
                    'season': final_season, 'date': pd.to_datetime(date_val), # ここで final_season, date_val が使われます
                    'environment': final_environment, 'format': final_format,
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
                    # --- ▼▼▼ リセット処理のデバッグ (inp_memo を pop で試す) ▼▼▼
                    
                    if 'inp_memo' in st.session_state:
                        try:
                            st.session_state.pop('inp_memo', None) # inp_memo を pop で削除
                            st.toast("inp_memo を pop でリセット試行しました。") # 動作確認用トースト
                        except Exception as e_memo:
                            # 通常 pop でこの種のエラーは起きにくいですが、念のため
                            st.error(f"inp_memo の pop でエラー: {e_memo}") 
                    
                    # 他の keys_to_reset_always_visible のループは一時的にコメントアウトのまま
                    # keys_to_reset_always_visible = { ... }
                    # for key, value in keys_to_reset_always_visible.items(): ...
                    
                    # _new で終わるキーのクリア処理 (pop を使っているのでこれは比較的安全なはず)
                    keys_to_pop_for_new_entry = [
                        'inp_season_new',
                        'inp_environment_new',
                        'inp_format_new',
                        'inp_my_deck_new',
                        'inp_my_deck_type_new',
                        'inp_opponent_deck_new',
                        'inp_opponent_deck_type_new'
                    ]
                    for key_to_pop in keys_to_pop_for_new_entry:
                        st.session_state.pop(key_to_pop, None) 
                    
                    # --- ▲▲▲ リセット処理のデバッグここまで ▲▲▲ ---
                    st.rerun()
                else:
                    error_placeholder.error("データの保存に失敗しました。Google Sheetsへの接続を確認してください。")
            # ... (final_season など、既存の値の取得はそのまま) ...
            final_my_class = st.session_state.get('inp_my_class')
            final_opponent_class = st.session_state.get('inp_opponent_class')

            # ... (エラーメッセージのチェックにクラスも追加) ...
            error_messages = []
            # ... (既存の必須チェック) ...
            if not final_my_class:
                error_messages.append("自分のクラスを選択してください。")
            if not final_opponent_class:
                 error_messages.append("相手のクラスを選択してください。")
            # ... (決着ターンのチェックなど) ...

            if error_messages:
                error_placeholder.error("、".join(error_messages))
                success_placeholder.empty()
            else:
                error_placeholder.empty()
                new_record_data = {
                    'season': final_season, 'date': pd.to_datetime(date_val),
                    'environment': final_environment, 'format': final_format,
                    'my_deck': final_my_deck, 'my_deck_type': final_my_deck_type,
                    'my_class': final_my_class, # my_class をデータに追加
                    'opponent_deck': final_opponent_deck, 'opponent_deck_type': final_opponent_deck_type,
                    'opponent_class': final_opponent_class, # opponent_class をデータに追加
                    'first_second': first_second_val, 'result': result_val,
                    'finish_turn': int(finish_turn_val) if finish_turn_val is not None else None,
                    'memo': memo_val
                }
                new_df_row = pd.DataFrame([new_record_data], columns=COLUMNS)
                if save_data(new_df_row, SPREADSHEET_ID, WORKSHEET_NAME):
                    success_placeholder.success("戦績を記録しました！")
                   



                    # --- ▲▲▲ リセット処理ここまで ▲▲▲ ---
                    st.rerun()
                else:
                    error_placeholder.error("データの保存に失敗しました。Google Sheetsへの接続を確認してください。")
    
    # --- show_analysis_section と 戦績一覧表示部分は、新しいクラス列を考慮した表示調整が必要になります ---
    # (今回は入力フォームの変更を主としていますが、後続で分析や一覧表示も修正します)
    show_analysis_section(df.copy())
    st.header("戦績一覧")
    if df.empty:
        st.info("まだ戦績データがありません。")
    else:
        display_columns = ['date', 'season', 'environment', 'format', 
                        'my_deck', 'my_deck_type', 'my_class', 
                        'opponent_deck', 'opponent_deck_type', 'opponent_class', 
                        'first_second', 'result', 'finish_turn', 'memo'] # クラス列を追加
        # ... (以降のデータフレーム表示ロジックは既存のものを流用し、新しい列が表示されるようにする) ...
        cols_to_display_actual = [col for col in display_columns if col in df.columns]
        df_display = df.copy()
        if 'date' in df_display.columns:
            df_display['date'] = pd.to_datetime(df_display['date'], errors='coerce')
            not_nat_dates = df_display.dropna(subset=['date'])
            nat_dates = df_display[df_display['date'].isna()]
            df_display_sorted = pd.concat([not_nat_dates.sort_values(by='date', ascending=False), nat_dates]).reset_index(drop=True)
            if pd.api.types.is_datetime64_any_dtype(df_display_sorted['date']):
                df_display_sorted['date'] = df_display_sorted['date'].apply(
                    lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        else:
            df_display_sorted = df_display.reset_index(drop=True)
        st.dataframe(df_display_sorted[cols_to_display_actual]) # ここで新しい列が表示される
        csv_export = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="戦績データをCSVでダウンロード", data=csv_export,
            file_name='game_records_download.csv', mime='text/csv',
        )

if __name__ == '__main__':
    main()