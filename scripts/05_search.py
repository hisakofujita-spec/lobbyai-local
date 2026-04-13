#!/usr/bin/env python3
"""
05_search.py - 議事録全文検索 CLI（営業優先度スコア付き）

使い方:
  python 05_search.py "産後ケア"
  python 05_search.py "病児保育" --limit 20
  python 05_search.py "伴走型相談支援" --prefecture 東京 --year 2024
  python 05_search.py "産後ケア事業" --output json
  python 05_search.py --stats
  python 05_search.py  # インタラクティブモード

営業優先度スコア:
  score = (過去2年間のキーワード言及回数 × 2) + (議員からの質問回数 × 3)
  A: score 10以上 / B: 5〜9 / C: 4以下
"""

import argparse
import json
import sqlite3
import sys
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "db" / "minutes.db"

DEFAULT_KEYWORDS = [
    # 病児保育関連
    "病児保育", "病児保育室", "病後児保育", "病児・病後児保育",
    "病児対応型", "保育所型病児保育", "訪問型病児保育",
    # 産後ケア関連
    "産後ケア", "産後ケア事業", "産後デイサービス", "産後ショートステイ",
    "産後アウトリーチ", "産後訪問", "産後支援", "産褥期ケア",
    "母子ケア", "産後うつ", "マタニティブルー",
    # その他
    "伴走型相談支援", "子育て支援",
]

SEP = "─" * 60
SEP_THICK = "━" * 60


# ──────────────────────────────────────────────
# DB 接続
# ──────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(f"エラー: DB が見つかりません: {DB_PATH}", file=sys.stderr)
        print("先に 04_indexer.py を実行してください。", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ──────────────────────────────────────────────
# 営業優先度スコア
# ──────────────────────────────────────────────

def _cutoff_date() -> str:
    """過去2年間の開始日（ISO形式）。"""
    return (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")


def calc_priority_scores(
    conn: sqlite3.Connection,
    keyword: str,
    municipality_names: list[str],
) -> dict[str, dict]:
    """
    各自治体の営業優先度スコアを計算して返す。

    返り値:
      { municipality_name: {score, grade, mention_count, question_count} }

    score = mention_count * 2 + question_count * 3
    grade: A(≥10) / B(5-9) / C(≤4)
    """
    cutoff = _cutoff_date()
    scores: dict[str, dict] = {}

    for name in municipality_names:
        # 過去2年間のキーワード言及件数（全発言者）
        mention_count: int = conn.execute(
            """
            SELECT COUNT(*) FROM minutes
            WHERE municipality_name = ?
              AND content LIKE ?
              AND meeting_date >= ?
            """,
            (name, f"%{keyword}%", cutoff),
        ).fetchone()[0]

        # 過去2年間の議員発言によるキーワード言及件数
        question_count: int = conn.execute(
            """
            SELECT COUNT(*) FROM minutes
            WHERE municipality_name = ?
              AND content LIKE ?
              AND meeting_date >= ?
              AND speaker LIKE '%議員%'
            """,
            (name, f"%{keyword}%", cutoff),
        ).fetchone()[0]

        score = mention_count * 2 + question_count * 3
        if score >= 10:
            grade = "A"
        elif score >= 5:
            grade = "B"
        else:
            grade = "C"

        scores[name] = {
            "score": score,
            "grade": grade,
            "mention_count": mention_count,
            "question_count": question_count,
        }

    return scores


def grade_label(grade: str) -> str:
    labels = {"A": "★★★ A（積極アプローチ推奨）",
               "B": "★★☆ B（要フォロー）",
               "C": "★☆☆ C（情報収集段階）"}
    return labels.get(grade, grade)


# ──────────────────────────────────────────────
# FTS5 検索
# ──────────────────────────────────────────────

def build_fts_query(keyword: str) -> str:
    keyword = keyword.strip()
    if " OR " in keyword.upper():
        parts = [p.strip() for p in keyword.split(" OR ")]
        return " OR ".join(f'"{p}"' for p in parts)
    elif " " in keyword:
        return " ".join(f'"{p}"' for p in keyword.split())
    return f'"{keyword}"'


def search(
    conn: sqlite3.Connection,
    keyword: str,
    limit: int = 10,
    prefecture: str = "",
    year: str = "",
) -> list[sqlite3.Row]:
    fts_query = build_fts_query(keyword)
    sql = """
        SELECT
            m.id, m.municipality_name, m.prefecture,
            m.meeting_name, m.meeting_date, m.speaker,
            m.content, m.source_url, m.fetched_at, rank
        FROM minutes_fts
        JOIN minutes m ON minutes_fts.rowid = m.id
        WHERE minutes_fts MATCH ?
    """
    params: list = [fts_query]
    if prefecture:
        sql += " AND (m.municipality_name LIKE ? OR m.prefecture LIKE ?)"
        params += [f"%{prefecture}%", f"%{prefecture}%"]
    if year:
        sql += " AND m.meeting_date LIKE ?"
        params.append(f"{year}%")
    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)

    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        # FTS が未構築の場合は LIKE フォールバック
        sql_fb = """
            SELECT id, municipality_name, prefecture, meeting_name,
                   meeting_date, speaker, content, source_url, fetched_at, 0 as rank
            FROM minutes WHERE content LIKE ?
        """
        fb_params = [f"%{keyword}%"]
        if prefecture:
            sql_fb += " AND (municipality_name LIKE ? OR prefecture LIKE ?)"
            fb_params += [f"%{prefecture}%", f"%{prefecture}%"]
        if year:
            sql_fb += " AND meeting_date LIKE ?"
            fb_params.append(f"{year}%")
        sql_fb += f" LIMIT {limit}"
        return conn.execute(sql_fb, fb_params).fetchall()


# ──────────────────────────────────────────────
# 表示ヘルパー
# ──────────────────────────────────────────────

def highlight_snippet(content: str, keyword: str) -> str:
    idx = content.find(keyword)
    if idx == -1:
        idx = content.lower().find(keyword.lower())
    if idx == -1:
        return content[:120] + "..."
    start = max(0, idx - 55)
    end = min(len(content), idx + len(keyword) + 55)
    snippet = content[start:end].replace("\n", " ")
    snippet = snippet.replace(keyword, f"【{keyword}】")
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet += "..."
    return snippet


def _row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


# ──────────────────────────────────────────────
# テキスト出力
# ──────────────────────────────────────────────

def format_text(
    rows: list[sqlite3.Row],
    keyword: str,
    scores: dict[str, dict],
):
    print(f"\nキーワード: {keyword}")
    print(f"検索件数: {len(rows)} 件  |  スコア算出期間: 過去2年")
    print(SEP_THICK)

    if not rows:
        print("ヒットなし")
        print(SEP_THICK)
        return

    # 営業優先度ごとにグルーピング
    by_grade: dict[str, list] = {"A": [], "B": [], "C": []}
    seen_names_per_grade: dict[str, set] = {"A": set(), "B": set(), "C": set()}

    for row in rows:
        name = row["municipality_name"] or "不明"
        sc = scores.get(name, {"grade": "C", "score": 0})
        by_grade[sc["grade"]].append(row)

    # ─── 優先度 A サマリー ───
    a_names = {}
    for row in by_grade["A"]:
        name = row["municipality_name"] or "不明"
        if name not in a_names:
            sc = scores[name]
            a_names[name] = sc

    if a_names:
        print(f"【営業優先度 A 自治体】  ← 今すぐアプローチ推奨")
        print(SEP)
        for name, sc in sorted(a_names.items(), key=lambda x: -x[1]["score"]):
            print(
                f"  ★ {name:<16}  "
                f"スコア {sc['score']:>3}  "
                f"（言及 {sc['mention_count']} 件 × 2  +  "
                f"議員質問 {sc['question_count']} 件 × 3）"
            )
        print(SEP_THICK)

    # ─── 全結果（グレード順） ───
    counter = 0
    for grade in ("A", "B", "C"):
        grade_rows = by_grade[grade]
        if not grade_rows:
            continue
        for row in grade_rows:
            counter += 1
            name = row["municipality_name"] or "不明"
            meeting = row["meeting_name"] or "（会議名不明）"
            date = row["meeting_date"] or "日付不明"
            speaker = row["speaker"] or "（発言者不明）"
            content = row["content"] or ""
            url = row["source_url"] or ""
            sc = scores.get(name, {"grade": "C", "score": 0,
                                    "mention_count": 0, "question_count": 0})

            snippet = highlight_snippet(content, keyword)
            wrapped = textwrap.fill(snippet, width=72, subsequent_indent="    ")

            print(f"[{counter}] {name} ／ {meeting} ／ {date}")
            print(f"    発言者: {speaker}")
            print(f"    {wrapped}")
            print(
                f"    優先度: {grade_label(sc['grade'])}  "
                f"(score={sc['score']}, 言及={sc['mention_count']}, 議員質問={sc['question_count']})"
            )
            if url:
                print(f"    URL: {url}")
            print(SEP)

    print(f"合計 {counter} 件表示")


# ──────────────────────────────────────────────
# JSON 出力
# ──────────────────────────────────────────────

def format_json(
    rows: list[sqlite3.Row],
    keyword: str,
    scores: dict[str, dict],
):
    results = []
    for row in rows:
        name = row["municipality_name"] or ""
        sc = scores.get(name, {"grade": "C", "score": 0,
                                "mention_count": 0, "question_count": 0})
        results.append({
            "id": row["id"],
            "municipality_name": name,
            "prefecture": row["prefecture"],
            "meeting_name": row["meeting_name"],
            "meeting_date": row["meeting_date"],
            "speaker": row["speaker"],
            "content": row["content"],
            "source_url": row["source_url"],
            "priority": {
                "grade": sc["grade"],
                "score": sc["score"],
                "mention_count": sc["mention_count"],
                "question_count": sc["question_count"],
            },
        })

    # A → B → C 順にソート
    results.sort(key=lambda r: (r["priority"]["grade"], -r["priority"]["score"]))

    print(json.dumps(
        {"keyword": keyword, "count": len(results), "results": results},
        ensure_ascii=False, indent=2,
    ))


# ──────────────────────────────────────────────
# 統計表示
# ──────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection):
    total = conn.execute("SELECT COUNT(*) FROM minutes").fetchone()[0]
    munis = conn.execute("SELECT COUNT(DISTINCT municipality_name) FROM minutes").fetchone()[0]
    date_range = conn.execute(
        "SELECT MIN(meeting_date), MAX(meeting_date) FROM minutes WHERE meeting_date != ''"
    ).fetchone()

    print(f"\n{'='*50}")
    print(f"  DB 統計")
    print(f"{'='*50}")
    print(f"  総レコード数:   {total:,}")
    print(f"  自治体数:       {munis:,}")
    if date_range[0]:
        print(f"  期間:           {date_range[0]} ～ {date_range[1]}")

    print(f"\n  自治体別レコード数 (上位20):")
    for name, cnt in conn.execute(
        "SELECT municipality_name, COUNT(*) FROM minutes "
        "GROUP BY municipality_name ORDER BY 2 DESC LIMIT 20"
    ):
        print(f"    {name:<18}: {cnt:>6,}")

    print(f"\n  年別レコード数:")
    for yr, cnt in conn.execute(
        "SELECT substr(meeting_date,1,4), COUNT(*) FROM minutes "
        "WHERE meeting_date != '' GROUP BY 1 ORDER BY 1 DESC LIMIT 10"
    ):
        print(f"    {yr}: {cnt:,}")

    print(f"\n  カテゴリ別自治体数:")
    try:
        for cat, cnt in conn.execute(
            "SELECT category, COUNT(DISTINCT municipality_name) "
            "FROM minutes LEFT JOIN "
            "(SELECT name, category FROM municipalities) m ON minutes.municipality_name=m.name "
            "GROUP BY category"
        ):
            print(f"    {cat or '不明'}: {cnt}")
    except Exception:
        pass


# ──────────────────────────────────────────────
# インタラクティブモード
# ──────────────────────────────────────────────

def interactive_mode(conn: sqlite3.Connection, limit: int, output: str):
    print("=== 議事録検索システム（営業優先度スコア付き）===")
    print(f"DB: {DB_PATH}")
    print(f"デフォルトキーワード例: {', '.join(DEFAULT_KEYWORDS)}")
    print("終了: Ctrl+C または quit")
    print(SEP_THICK)

    while True:
        try:
            keyword = input("\n検索キーワード > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n終了します。")
            break
        if keyword.lower() in ("quit", "exit", "q"):
            break
        if not keyword:
            continue

        rows = search(conn, keyword, limit=limit)
        names = list({row["municipality_name"] for row in rows if row["municipality_name"]})
        scores = calc_priority_scores(conn, keyword, names)

        if output == "json":
            format_json(rows, keyword, scores)
        else:
            format_text(rows, keyword, scores)


# ──────────────────────────────────────────────
# エントリーポイント
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="自治体議会議事録 全文検索（営業優先度スコア付き）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            使用例:
              python 05_search.py "産後ケア"
              python 05_search.py "病児保育" --limit 20
              python 05_search.py "伴走型相談支援" --year 2024
              python 05_search.py "産後ケア事業" --output json
              python 05_search.py --stats
        """),
    )
    parser.add_argument("keyword", nargs="?",
                        help="検索キーワード（省略でインタラクティブモード）")
    parser.add_argument("--limit", type=int, default=10,
                        help="表示件数（デフォルト: 10）")
    parser.add_argument("--prefecture", default="",
                        help="都道府県・市名でフィルタ（例: 東京）")
    parser.add_argument("--year", default="",
                        help="年でフィルタ（例: 2024）")
    parser.add_argument("--output", choices=["text", "json"], default="text",
                        help="出力形式（デフォルト: text）")
    parser.add_argument("--stats", action="store_true",
                        help="DB 統計を表示")
    args = parser.parse_args()

    conn = open_db()

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    if args.keyword:
        rows = search(conn, args.keyword, limit=args.limit,
                      prefecture=args.prefecture, year=args.year)
        names = list({row["municipality_name"] for row in rows if row["municipality_name"]})
        scores = calc_priority_scores(conn, args.keyword, names)

        if args.output == "json":
            format_json(rows, args.keyword, scores)
        else:
            format_text(rows, args.keyword, scores)
    else:
        interactive_mode(conn, limit=args.limit, output=args.output)

    conn.close()


if __name__ == "__main__":
    main()
