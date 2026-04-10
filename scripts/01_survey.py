#!/usr/bin/env python3
"""
01_survey.py - 自治体議会サイト調査スクリプト
municipalities.csv を読み込み、各URLのアクセス可否・構造を調査して
survey_report.csv に出力する。
"""

import csv
import re
import sys
import time
import urllib.parse
from pathlib import Path

import warnings
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

BASE_DIR = Path(__file__).parent.parent
INPUT_CSV = BASE_DIR / "data" / "municipalities.csv"
OUTPUT_CSV = BASE_DIR / "reports" / "survey_report.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LobbyAI-Survey/1.0; "
        "+https://github.com/lobbyai/local-assembly-minutes)"
    )
}
REQUEST_TIMEOUT = 15
SLEEP_BETWEEN = 1.5  # 過負荷防止のインターバル（秒）

# 動的サイトの特徴的なキーワード
DYNAMIC_SIGNALS = [
    "angular", "vue", "react", "next.js", "nuxt",
    "__NEXT_DATA__", "ng-app", "data-v-",
    "application/json", "XMLHttpRequest",
]

# PDFリンクの判定パターン
PDF_LINK_PATTERN = re.compile(r'href=["\'][^"\']*\.pdf["\']', re.IGNORECASE)


def fetch_robots_txt(base_url: str) -> str:
    """robots.txt を取得して内容を返す。取得失敗時は空文字。"""
    parsed = urllib.parse.urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        resp = requests.get(robots_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        return ""
    except Exception:
        return ""


def check_robots_allowed(robots_txt: str, path: str = "/") -> str:
    """
    robots.txt でクローリングが許可されているか簡易チェック。
    返り値: "yes" / "no" / "unknown"
    """
    if not robots_txt:
        return "unknown"

    user_agent_section = False
    disallow_all = False

    for line in robots_txt.splitlines():
        line = line.strip()
        if line.lower().startswith("user-agent:"):
            ua = line.split(":", 1)[1].strip()
            user_agent_section = ua in ("*", "LobbyAI-Survey")
        elif user_agent_section and line.lower().startswith("disallow:"):
            disallow_path = line.split(":", 1)[1].strip()
            if disallow_path == "/" or path.startswith(disallow_path):
                disallow_all = True

    if disallow_all:
        return "no"
    return "yes"


def fetch_page(url: str) -> tuple[int, str]:
    """
    HTMLを取得する。
    返り値: (status_code, html_text)  失敗時は (0, "")
    """
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True
        )
        return resp.status_code, resp.text
    except Exception:
        return 0, ""


def detect_access_type(html: str, url: str) -> tuple[str, str]:
    """
    ページ構造からアクセスタイプと構造メモを判定する。
    返り値: (access_type, structure_notes)
    access_type: "static" / "dynamic" / "pdf" / "unknown"
    """
    if not html:
        return "unknown", "取得失敗またはタイムアウト"

    html_lower = html.lower()

    # PDF直リンクが多ければ pdf
    pdf_links = PDF_LINK_PATTERN.findall(html)
    if len(pdf_links) >= 3:
        return "pdf", f"PDFリンク {len(pdf_links)} 件検出"

    # 動的フレームワークの検出
    for signal in DYNAMIC_SIGNALS:
        if signal.lower() in html_lower:
            return "dynamic", f"動的シグナル検出: {signal}"

    # iframe で外部検索システムへ飛ばしている場合
    soup = BeautifulSoup(html, "lxml")
    iframes = soup.find_all("iframe")
    for iframe in iframes:
        src = iframe.get("src", "")
        if any(
            domain in src
            for domain in ["kaigiroku.net", "gijiroku.com", "dbsr.jp", "kensakusystem.jp"]
        ):
            return "dynamic", f"外部検索システムへiframe埋め込み: {src[:80]}"

    # 既知の外部議事録システムのURLが minutes_url に含まれている場合
    external_systems = [
        "kaigiroku.net", "gijiroku.com", "dbsr.jp",
        "kensakusystem.jp", "gsl-service.net",
    ]
    for system in external_systems:
        if system in url:
            return "dynamic", f"外部議事録システム: {system}"

    # ページ内に議事録・会議録リンクがあれば static
    keywords = ["会議録", "議事録", "会議記録", "本会議", "委員会"]
    for kw in keywords:
        if kw in html:
            links = soup.find_all("a", href=True)
            minutes_links = [
                a for a in links
                if any(ext in a.get("href", "").lower() for ext in [".html", ".htm", ".pdf"])
            ]
            if minutes_links:
                return "static", f"静的HTMLリンク {len(minutes_links)} 件、キーワード '{kw}' 検出"

    return "static", "構造判定: 静的HTML（議事録リンク未検出）"


def estimate_difficulty(access_type: str, robots_ok: str, structure_notes: str) -> str:
    """
    難易度を A〜D で判定する。
    A=直接取得可, B=PDF変換必要, C=動的JS必要, D=技術的困難
    """
    if robots_ok == "no":
        return "D"
    if access_type == "unknown":
        return "D"
    if access_type == "pdf":
        return "B"
    if access_type == "dynamic":
        # 既知の外部システムは構造が共通なので C
        return "C"
    # static
    return "A"


def survey_municipality(row: dict, index: int, total: int) -> dict:
    """1件の自治体を調査して結果を返す。"""
    name = row["name"]
    assembly_url = row["assembly_url"]
    minutes_url = row["minutes_url"]

    print(f"[{index:02d}/{total:02d}] {name} を調査中...")

    # robots.txt チェック
    robots_txt = fetch_robots_txt(minutes_url or assembly_url)
    parsed = urllib.parse.urlparse(minutes_url or assembly_url)
    robots_ok = check_robots_allowed(robots_txt, parsed.path or "/")

    # HTMLを取得
    target_url = minutes_url if minutes_url else assembly_url
    status_code, html = fetch_page(target_url)

    if status_code == 0:
        print(f"  -> 取得失敗")
        return {
            "name": name,
            "assembly_url": assembly_url,
            "minutes_url": minutes_url,
            "access_type": "unknown",
            "robots_txt_ok": robots_ok,
            "structure_notes": "接続失敗（タイムアウトまたはネットワークエラー）",
            "difficulty": "D",
        }
    elif status_code != 200:
        print(f"  -> HTTP {status_code}")
        return {
            "name": name,
            "assembly_url": assembly_url,
            "minutes_url": minutes_url,
            "access_type": "unknown",
            "robots_txt_ok": robots_ok,
            "structure_notes": f"HTTP {status_code}",
            "difficulty": "D",
        }

    access_type, structure_notes = detect_access_type(html, target_url)
    difficulty = estimate_difficulty(access_type, robots_ok, structure_notes)

    print(f"  -> {access_type} / robots:{robots_ok} / 難易度:{difficulty}")
    return {
        "name": name,
        "assembly_url": assembly_url,
        "minutes_url": minutes_url,
        "access_type": access_type,
        "robots_txt_ok": robots_ok,
        "structure_notes": structure_notes,
        "difficulty": difficulty,
    }


def main():
    if not INPUT_CSV.exists():
        print(f"エラー: {INPUT_CSV} が見つかりません", file=sys.stderr)
        sys.exit(1)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(INPUT_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        municipalities = list(reader)

    total = len(municipalities)
    print(f"調査対象: {total} 件")
    print("=" * 60)

    results = []
    for i, row in enumerate(municipalities, start=1):
        result = survey_municipality(row, i, total)
        results.append(result)
        if i < total:
            time.sleep(SLEEP_BETWEEN)

    print("=" * 60)
    print(f"調査完了。結果を {OUTPUT_CSV} に書き込み中...")

    fieldnames = [
        "name", "assembly_url", "minutes_url",
        "access_type", "robots_txt_ok", "structure_notes", "difficulty",
    ]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # サマリー表示
    difficulty_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    for r in results:
        difficulty_counts[r["difficulty"]] = difficulty_counts.get(r["difficulty"], 0) + 1

    print(f"\n--- 難易度サマリー ---")
    print(f"  A (直接取得可):     {difficulty_counts['A']:2d} 件")
    print(f"  B (PDF変換必要):    {difficulty_counts['B']:2d} 件")
    print(f"  C (動的JS必要):     {difficulty_counts['C']:2d} 件")
    print(f"  D (技術的困難):     {difficulty_counts['D']:2d} 件")
    print(f"\n出力: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
