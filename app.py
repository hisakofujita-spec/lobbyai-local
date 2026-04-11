#!/usr/bin/env python3
"""
app.py - 議事録全文検索 Web アプリ (Flask)

起動: python3 app.py
アクセス: http://localhost:5000
"""

import os
import re
import sqlite3

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "db", "minutes.db")
RESULTS_LIMIT = 50
SNIPPET_WINDOW = 120  # キーワード前後の文字数


# ---------------------------------------------------------------------------
# DB ヘルパー
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    uri = "file:{}?mode=ro".format(DB_PATH.replace("\\", "/"))
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def extract_snippet(content: str, query: str) -> str:
    """クエリのキーワードが含まれる前後 SNIPPET_WINDOW 文字を返す。"""
    if not content:
        return ""
    # 最初のキーワードの位置を探す
    for word in query.split():
        idx = content.lower().find(word.lower())
        if idx >= 0:
            start = max(0, idx - SNIPPET_WINDOW // 2)
            end = min(len(content), idx + len(word) + SNIPPET_WINDOW // 2)
            snippet = content[start:end]
            if start > 0:
                snippet = "…" + snippet
            if end < len(content):
                snippet = snippet + "…"
            return snippet
    # 見つからなければ先頭を返す
    return content[: SNIPPET_WINDOW * 2] + ("…" if len(content) > SNIPPET_WINDOW * 2 else "")


def calc_grade_score(rank: float, best_rank: float, worst_rank: float) -> tuple[str, int]:
    """
    FTS5 rank（負の値、小さいほど良い）を正規化してスコアとグレードを返す。
    score: 0–100、grade: A / B / C
    """
    span = worst_rank - best_rank
    if span == 0:
        norm = 1.0
    else:
        norm = (worst_rank - rank) / span  # 0〜1、大きいほど良い
    score = round(norm * 100)
    if score >= 67:
        grade = "A"
    elif score >= 34:
        grade = "B"
    else:
        grade = "C"
    return grade, score


# ---------------------------------------------------------------------------
# ルート
# ---------------------------------------------------------------------------

@app.route("/debug")
def debug():
    import os
    return {
        "cwd": os.getcwd(),
        "file": __file__,
        "db_path": str(DB_PATH),
        "db_exists": os.path.exists(DB_PATH),
        "dir_contents": os.listdir(os.path.dirname(DB_PATH)) if os.path.exists(os.path.dirname(DB_PATH)) else "dir not found",
        "root_contents": os.listdir("/var/task") if os.path.exists("/var/task") else "no /var/task",
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"results": [], "total": 0, "query": ""})

    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                m.id,
                m.municipality_name,
                m.prefecture,
                m.meeting_name,
                m.meeting_date,
                m.speaker,
                m.content,
                m.source_url,
                minutes_fts.rank AS rank
            FROM minutes_fts
            JOIN minutes m ON minutes_fts.rowid = m.id
            WHERE minutes_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, RESULTS_LIMIT),
        ).fetchall()
    except Exception as e:
        return jsonify({"error": str(e), "results": [], "total": 0, "query": query})
    finally:
        conn.close()

    if not rows:
        return jsonify({"results": [], "total": 0, "query": query})

    ranks = [r["rank"] for r in rows]
    best_rank = min(ranks)
    worst_rank = max(ranks)

    results = []
    for row in rows:
        grade, score = calc_grade_score(row["rank"], best_rank, worst_rank)
        snippet = extract_snippet(row["content"] or "", query)
        results.append(
            {
                "municipality_name": row["municipality_name"],
                "meeting_name": row["meeting_name"] or "—",
                "meeting_date": row["meeting_date"] or "—",
                "speaker": row["speaker"] or "—",
                "snippet": snippet,
                "grade": grade,
                "score": score,
                "source_url": row["source_url"] or "",
            }
        )

    return jsonify({"results": results, "total": len(results), "query": query})


@app.route("/stats")
def stats():
    conn = get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM minutes").fetchone()[0]
        by_name = conn.execute(
            """
            SELECT municipality_name, COUNT(*) AS cnt
            FROM minutes
            GROUP BY municipality_name
            ORDER BY cnt DESC
            """
        ).fetchall()
        return jsonify(
            {
                "total": total,
                "by_municipality": [
                    {"name": r["municipality_name"], "count": r["cnt"]} for r in by_name
                ],
            }
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"エラー: DB が見つかりません → {DB_PATH}")
        print("先に python3 scripts/04_indexer.py を実行してください。")
    else:
        print(f"DB: {DB_PATH}")
        print("起動: http://localhost:5001")
        app.run(debug=True, port=5001)
