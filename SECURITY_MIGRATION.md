# Streamlit へシークレット移行と鍵ローテーション手順

このリポジトリで検出された機密情報（サービスアカウント JSON、プライベートキー、ハードコーディングされた SPREADSHEET_ID 等）を Streamlit の Secrets に移行し、安全に運用するための手順をまとめます。

注意（優先度高）
- 履歴に含まれるサービスアカウント鍵は既に外部へ露出している可能性があります。まず GCP コンソールで該当キーを無効化（削除）し、必要なら新しいキーを作成してアプリに反映してください。キーの無効化は最優先です。

短い手順（実行の推奨順）
1. GCP でキーを無効化／削除し、新しいサービスアカウントキーを発行する（管理者作業）
   - Google Cloud Console → IAM & Admin → Service Accounts → 該当サービスアカウント → Keys から古いキーを削除して新規キーを作成（JSON をダウンロード）。

2. Streamlit (Cloud) の Secrets に設定する
   - Streamlit Cloud のアプリページ → Settings → Secrets へ移動。
   - 推奨の登録方法（2つの選択肢）:
     (A) JSON をネイティブなテーブルとして登録（推奨）:
         secrets.toml の例（Streamlit の UI ではキーに `gcp_service_account` を作り、値に JSON のキー/値を入れる）:

         [gcp_service_account]
         type = "service_account"
         project_id = "your-project-id"
         private_key_id = "xxxxxxxxxxxxxxxx"
         private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
         client_email = "...@...iam.gserviceaccount.com"
         client_id = "..."
         auth_uri = "https://accounts.google.com/o/oauth2/auth"
         token_uri = "https://oauth2.googleapis.com/token"
         # もし置きたい場合はここに SPREADSHEET_ID も追加できます
         SPREADSHEET_ID = "1V9guZ..."

     (B) 単一の環境変数に JSON を文字列で格納（簡易）:
         Key: GCP_SERVICE_ACCOUNT_JSON
         Value: <ダウンロードした service-account.json の全文を1行またはエスケープして貼り付け>

   - さらに Streamlit Secrets に `SPREADSHEET_ID` と `WORKSHEET_NAME` を登録する（上の gcp_service_account に入れても可）。

3. リポジトリの修正確認
   - このリポジトリでは `card_tracker.py` が `st.secrets['gcp_service_account']` を使うように、`gspread_test.py` も st.secrets / 環境変数 / ローカルの順で認証情報を取得するように修正済みです。
   - デプロイ前に Streamlit 上で `st.secrets` に正しい値が入っていることを確認してください。

4. 履歴の機密情報除去（オプションだが推奨）
   - リポジトリ履歴に機密が残っているので、git の履歴書き換え（git-filter-repo か BFG）で削除するのが望ましいです。
   - 簡易コマンド例（ローカル、PowerShell）:

     # ミラークローンを作成
     git clone --mirror https://github.com/<user>/<repo>.git
     cd <repo>.git
     # git-filter-repo を使って特定ファイルを削除
     git filter-repo --invert-paths --paths service_account.json --paths streamlit_test_env --paths "**/*.key"
     # リモートに強制プッシュ
     git push --force --all
     git push --force --tags

   - 注意: 履歴書き換えは破壊的です。クローンを持つ全員に影響します。実行前にチームへ周知し、バックアップを取り、全員が再クローンできるようにしてください。

5. 検証と再デプロイ
   - Streamlit 上でアプリを再デプロイし、動作確認（Google Sheets に読み書きできるか）を行ってください。

6. 追加で私がやれること
   - Streamlit に必要なシークレット名とテンプレを作ります（済）。
   - 履歴書き換え（git-filter-repo）を代行して実行することも可能ですが、強制プッシュの許可とチーム同意が必要です。

---
短くまとめると：
1) 鍵のローテーション（GCP）を最優先で行ってください。 2) 新しい JSON を Streamlit Secrets に設定。3) リモートの履歴をきれいにする（必要なら私がサポート）。

