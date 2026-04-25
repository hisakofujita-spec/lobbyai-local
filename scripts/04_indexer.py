#!/usr/bin/env python3
"""
04_indexer.py - SQLite FTS5 インデクサー

data/parsed/*/minutes.json を読み込み、SQLite DB（db/minutes.db）に格納する。
FTS5 仮想テーブルを使って全文検索を有効化する。
"""

import csv
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
PARSED_DIR = BASE_DIR / "data" / "parsed"
DB_PATH = BASE_DIR / "db" / "minutes.db"
MUNICIPALITIES_CSV = BASE_DIR / "data" / "municipalities.csv"


def load_muni_code_map() -> dict[str, str]:
    """municipalities.csv から 自治体名 → 都道府県コード マッピングを構築。"""
    mapping: dict[str, str] = {}
    if not MUNICIPALITIES_CSV.exists():
        return mapping
    with open(MUNICIPALITIES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mapping[row["name"]] = row["prefecture_code"]
    return mapping

DDL_MINUTES = """
CREATE TABLE IF NOT EXISTS minutes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    municipality_name TEXT NOT NULL,
    prefecture  TEXT DEFAULT '',
    meeting_name TEXT DEFAULT '',
    meeting_date TEXT DEFAULT '',
    speaker     TEXT DEFAULT '',
    content     TEXT NOT NULL,
    source_url  TEXT DEFAULT '',
    fetched_at  TEXT DEFAULT ''
);
"""

DDL_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS minutes_fts USING fts5(
    municipality_name,
    meeting_name,
    meeting_date,
    speaker,
    content,
    content=minutes,
    content_rowid=id,
    tokenize='trigram'
);
"""

DDL_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS minutes_ai
AFTER INSERT ON minutes BEGIN
    INSERT INTO minutes_fts(rowid, municipality_name, meeting_name, meeting_date, speaker, content)
    VALUES (new.id, new.municipality_name, new.meeting_name, new.meeting_date, new.speaker, new.content);
END;

CREATE TRIGGER IF NOT EXISTS minutes_ad
AFTER DELETE ON minutes BEGIN
    INSERT INTO minutes_fts(minutes_fts, rowid, municipality_name, meeting_name, meeting_date, speaker, content)
    VALUES ('delete', old.id, old.municipality_name, old.meeting_name, old.meeting_date, old.speaker, old.content);
END;

CREATE TRIGGER IF NOT EXISTS minutes_au
AFTER UPDATE ON minutes BEGIN
    INSERT INTO minutes_fts(minutes_fts, rowid, municipality_name, meeting_name, meeting_date, speaker, content)
    VALUES ('delete', old.id, old.municipality_name, old.meeting_name, old.meeting_date, old.speaker, old.content);
    INSERT INTO minutes_fts(rowid, municipality_name, meeting_name, meeting_date, speaker, content)
    VALUES (new.id, new.municipality_name, new.meeting_name, new.meeting_date, new.speaker, new.content);
END;
"""

DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_minutes_name ON minutes(municipality_name);
CREATE INDEX IF NOT EXISTS idx_minutes_date ON minutes(meeting_date);
CREATE INDEX IF NOT EXISTS idx_minutes_speaker ON minutes(speaker);
"""


def init_db(conn: sqlite3.Connection):
    conn.executescript(DDL_MINUTES)
    conn.executescript(DDL_FTS)
    conn.executescript(DDL_TRIGGERS)
    conn.executescript(DDL_INDEXES)
    conn.commit()


def get_existing_urls(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT DISTINCT source_url FROM minutes").fetchall()
    return {r[0] for r in rows}


def insert_records(conn: sqlite3.Connection, records: list[dict], existing_urls: set[str]) -> int:
    """
    新規レコードのみ挿入（source_url で重複チェック）。
    挿入件数を返す。
    """
    sql = """
        INSERT INTO minutes
            (municipality_name, prefecture, meeting_name, meeting_date,
             speaker, content, source_url, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    count = 0
    for r in records:
        url = r.get("source_url", "")
        if url and url in existing_urls:
            continue
        conn.execute(sql, (
            r.get("municipality_name", ""),
            r.get("prefecture", ""),
            r.get("meeting_name", ""),
            r.get("meeting_date", ""),
            r.get("speaker", ""),
            r.get("content", ""),
            url,
            r.get("fetched_at", datetime.utcnow().isoformat()),
        ))
        existing_urls.add(url)
        count += 1
    return count


def rebuild_fts(conn: sqlite3.Connection):
    """FTS インデックスを全再構築する（content テーブルとの同期）。"""
    print("FTS インデックスを再構築中...")
    conn.execute("INSERT INTO minutes_fts(minutes_fts) VALUES('rebuild')")
    conn.commit()
    print("FTS インデックス再構築完了")


def load_json_file(path: Path) -> list[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"  JSON 読み込みエラー ({path}): {e}")
        return []


def print_stats(conn: sqlite3.Connection):
    total = conn.execute("SELECT COUNT(*) FROM minutes").fetchone()[0]
    by_name = conn.execute(
        "SELECT municipality_name, COUNT(*) as cnt FROM minutes "
        "GROUP BY municipality_name ORDER BY cnt DESC LIMIT 20"
    ).fetchall()
    print(f"\n--- DB 統計 ---")
    print(f"  総レコード数: {total:,}")
    print(f"  自治体別上位:")
    for name, cnt in by_name:
        print(f"    {name}: {cnt:,}")


def update_prefecture_codes(conn: sqlite3.Connection, muni_code_map: dict[str, str]) -> int:
    """prefecture が空のレコードに都道府県コードを補完する。更新件数を返す。"""
    updated = 0
    for name, code in muni_code_map.items():
        result = conn.execute(
            "UPDATE minutes SET prefecture = ? WHERE municipality_name = ? AND (prefecture = '' OR prefecture IS NULL)",
            (code, name),
        )
        updated += result.rowcount
    conn.commit()
    return updated


def main(db_path=None):
    target = db_path if db_path is not None else DB_PATH

    json_files = sorted(PARSED_DIR.glob("*/minutes.json"))

    if not json_files:
        print(f"解析済みファイルが見つかりません: {PARSED_DIR}")
        print("先に 02_scraper.py（および必要に応じて 03_pdf_parser.py）を実行してください。")
        sys.exit(0)

    muni_code_map = load_muni_code_map()

    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    print("DB を初期化中...")
    init_db(conn)

    existing_urls = get_existing_urls(conn)
    print(f"既存レコード source_url: {len(existing_urls)} 件")
    print(f"JSONファイル: {len(json_files)} 件")
    print("=" * 60)

    total_inserted = 0

    for i, json_file in enumerate(json_files, start=1):
        name = json_file.parent.name
        print(f"[{i:02d}/{len(json_files):02d}] {name}")
        records = load_json_file(json_file)
        if not records:
            print(f"  -> レコードなし")
            continue
        # JSONにprefectureがなければCSVから補完
        code = muni_code_map.get(name, "")
        if code:
            for r in records:
                if not r.get("prefecture"):
                    r["prefecture"] = code
        inserted = insert_records(conn, records, existing_urls)
        conn.commit()
        print(f"  -> {inserted} 件挿入（ファイル内 {len(records)} 件）")
        total_inserted += inserted

    print("\n" + "=" * 60)
    print(f"挿入完了: {total_inserted} 件")

    if total_inserted > 0:
        rebuild_fts(conn)

    # 既存レコードのprefectureが空のものを一括補完
    if muni_code_map:
        updated = update_prefecture_codes(conn, muni_code_map)
        if updated:
            print(f"都道府県コード補完: {updated} 件更新")

    print_stats(conn)
    # WAL を完全にマージして immutable モードで安全に読めるようにする
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    print(f"\nDB: {target}")
    print(f"次のステップ: python 05_search.py \"産後ケア\"")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=str, help="出力先 DB パス（省略時: db/minutes.db）")
    args = parser.parse_args()
    main(db_path=Path(args.db_path) if args.db_path else None)
