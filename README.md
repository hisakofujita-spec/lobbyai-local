# LobbyAI Local — 自治体議会議事録 横断検索システム

地方議会の議事録を自動収集・全文検索し、**営業優先度スコア**でアプローチ先を特定するシステム。

---

## カバレッジ

| カテゴリ | 件数 | 概要 |
|---|---|---|
| 都道府県議会 | 47 | 全都道府県 |
| 政令指定都市 | 20 | 札幌〜熊本 |
| 中核市 | 62 | 総務省指定（2024年時点） |
| 人口10万以上市 | 約20 | 政令市・中核市未指定 |
| **合計** | **約150** | |

議事録システム別内訳：`ssp.kaigiroku.net`（最多）、`*.dbsr.jp`、`*.gijiroku.com`、`kensakusystem.jp`、独自システム

---

## セットアップ

### 必要環境
- Python 3.11 以上
- macOS / Linux / Windows（WSL推奨）

### インストール

```bash
git clone <repo>
cd lobbyai-local
pip install -r scripts/requirements.txt
```

### 初回実行（フルパイプライン）

```bash
# Step 1: 自治体サイトの調査（robots.txt確認・難易度分類）
python scripts/01_survey.py
# → reports/survey_report.csv に出力

# Step 2: 議事録の収集（difficulty=A→Bの順）
python scripts/02_scraper.py
# → data/parsed/*/minutes.json（静的HTML）
# → data/raw/*/*.pdf（PDF）

# Step 3: PDFのテキスト抽出
python scripts/03_pdf_parser.py
# → data/parsed/*/minutes.json に追記

# Step 4: SQLite DBに格納
python scripts/04_indexer.py
# → db/minutes.db
```

---

## 検索の使い方

```bash
# 基本検索
python scripts/05_search.py "産後ケア"

# 件数・絞り込み
python scripts/05_search.py "病児保育" --limit 20
python scripts/05_search.py "伴走型相談支援" --year 2024
python scripts/05_search.py "産後ケア事業" --prefecture 大阪

# JSON出力（他システムとの連携用）
python scripts/05_search.py "子育て支援" --output json

# DB統計
python scripts/05_search.py --stats

# インタラクティブモード
python scripts/05_search.py
```

### 出力例

```
キーワード: 産後ケア
検索件数: 8 件  |  スコア算出期間: 過去2年
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【営業優先度 A 自治体】  ← 今すぐアプローチ推奨
─────────────────────────────────────────────────────────────
  ★ 横浜市            スコア  14  （言及 4 件 × 2  +  議員質問 2 件 × 3）
  ★ 大阪市            スコア  10  （言及 2 件 × 2  +  議員質問 2 件 × 3）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[1] 横浜市 ／ 令和6年9月定例会 ／ 2024-09-15
    発言者: 田中議員
    ...市内における【産後ケア】事業の利用者数は前年比120%に増加し...
    優先度: ★★★ A（積極アプローチ推奨）  (score=14, 言及=4, 議員質問=2)
    URL: https://...
```

### 営業優先度スコアの計算ロジック

```
score = (過去2年間のキーワード言及件数 × 2)
      + (議員からの質問件数 × 3)

A: score ≥ 10  → 積極アプローチ推奨
B: score 5〜9  → 要フォロー
C: score ≤ 4   → 情報収集段階
```

---

## 定期更新

### cron（Linux/macOS）

```bash
# crontab -e で以下を追加
# 毎週月曜 AM6:00 に実行
0 6 * * 1 cd /path/to/lobbyai-local && SLACK_WEBHOOK_URL=https://hooks.slack.com/... python scripts/06_scheduler.py >> logs/cron.log 2>&1
```

### GitHub Actions

`.github/workflows/update.yml` が設定済みです。

1. リポジトリを GitHub にプッシュ
2. **Settings → Secrets → Actions** に `SLACK_WEBHOOK_URL` を追加
3. 毎週月曜 AM6:00 JST に自動実行

手動実行: Actions タブ → `議事録定期更新` → `Run workflow`

### Slack 通知

```bash
# 環境変数に設定
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../..."

# テスト（スクレイプなし）
python scripts/06_scheduler.py --dry-run
```

---

## ファイル構成

```
lobbyai-local/
├── data/
│   ├── municipalities.csv    # 自治体マスター（約150件）
│   ├── raw/                  # ダウンロードしたPDF
│   └── parsed/               # 抽出済みJSON
├── db/
│   └── minutes.db            # SQLite全文検索DB（FTS5）
├── logs/                     # パイプライン実行ログ
├── reports/
│   └── survey_report.csv     # サイト調査結果
├── scripts/
│   ├── 01_survey.py          # サイト調査（robots.txt・難易度分類）
│   ├── 02_scraper.py         # 議事録収集（A:HTML/B:PDF）
│   ├── 03_pdf_parser.py      # PDF→テキスト抽出（pdfplumber）
│   ├── 04_indexer.py         # SQLite FTS5インデクサー
│   ├── 05_search.py          # 検索CLI（営業優先度スコア付き）
│   ├── 06_scheduler.py       # 定期更新パイプライン＋Slack通知
│   └── requirements.txt
└── .github/
    └── workflows/
        └── update.yml        # GitHub Actions定期実行設定
```

---

## Web アプリ（ローカル起動）

```bash
pip install flask
python3 app.py
# → http://localhost:5001 をブラウザで開く
```

---

## Vercel デプロイ

### 前提
- GitHub アカウントと Vercel アカウント（どちらも無料プランで可）
- `db/minutes.db` はリポジトリに含める構成（Vercel が直接参照）

### 手順

#### 1. GitHub リポジトリを作成してプッシュ

```bash
cd lobbyai-local
git init
git add .
git commit -m "initial commit"

# GitHub で新規リポジトリ作成後（例: https://github.com/yourname/lobbyai-local）
git remote add origin https://github.com/yourname/lobbyai-local.git
git branch -M main
git push -u origin main
```

> **注意**: `data/raw/` は `.gitignore` で除外済み（171MB のPDF群）。  
> `db/minutes.db`（14MB）は含まれます。GitHub の 100MB 制限内です。

#### 2. Vercel にインポート

1. [vercel.com](https://vercel.com) → **Add New Project**
2. GitHub リポジトリ `lobbyai-local` を選択
3. **Framework Preset**: `Other`（自動検出されるはず）
4. **Root Directory**: そのまま（変更不要）
5. **Deploy** をクリック

`vercel.json` と `requirements.txt` が検出され、自動的に Python ランタイムでビルドされます。

#### 3. デプロイ完了

Vercel が発行する URL（例: `https://lobbyai-local.vercel.app`）でアクセス可能になります。

#### データ更新時の再デプロイ

```bash
# ローカルで議事録を更新後
python3 scripts/02_scraper.py
python3 scripts/04_indexer.py

# DB を push するだけで自動再デプロイ
git add db/minutes.db
git commit -m "update minutes.db"
git push
```

---

## SQLiteスキーマ

```sql
CREATE TABLE minutes (
  id                INTEGER PRIMARY KEY,
  municipality_name TEXT,    -- 自治体名
  prefecture        TEXT,    -- 都道府県
  meeting_name      TEXT,    -- 会議名（例: 令和6年9月定例会）
  meeting_date      TEXT,    -- 会議日（ISO形式: 2024-09-15）
  speaker           TEXT,    -- 発言者
  content           TEXT,    -- 発言内容
  source_url        TEXT,    -- 出典URL
  fetched_at        TEXT     -- 取得日時
);

CREATE VIRTUAL TABLE minutes_fts USING fts5(
  municipality_name, meeting_name, meeting_date, speaker, content,
  content=minutes, content_rowid=id
);
```

---

## 難易度分類

| 難易度 | 意味 | 主なシステム |
|---|---|---|
| A | 直接取得可（静的HTML） | 県庁内PDFリンク、独自HTML |
| B | PDF変換必要 | 会議録PDF |
| C | 動的JS必要 | kaigiroku.net、gijiroku.com、dbsr.jp |
| D | 技術的困難 | 画像PDF、要認証、robots.txt禁止 |

> **Phase 3以降**: difficulty=Cの外部システム（kaigiroku.net等）へのAPI対応を予定。

---

## 依存パッケージ

| パッケージ | 用途 |
|---|---|
| `requests` | HTTP取得 |
| `beautifulsoup4` + `lxml` | HTML解析 |
| `pdfplumber` | PDF→テキスト抽出 |

標準ライブラリのみ: `sqlite3`、`urllib`、`subprocess`、`csv`、`json`
