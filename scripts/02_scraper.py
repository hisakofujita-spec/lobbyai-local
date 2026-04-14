#!/usr/bin/env python3
"""
02_scraper.py - 議事録スクレイパー

survey_report.csv を読み込み、difficulty=A/B/C の自治体の議事録を収集する。
- A: HTMLを解析して議事録テキストを抽出 → data/parsed/{name}/*.json
- B: PDFをダウンロード → data/raw/{name}/*.pdf（03_pdf_parser.py で処理）
- C: gijiroku.com / kensakusystem.jp 等の外部システムを直接APIアクセス
"""

import csv
import json
import re
import sys
import time
import urllib.parse
import warnings
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

BASE_DIR = Path(__file__).parent.parent
SURVEY_CSV = BASE_DIR / "reports" / "survey_report.csv"
MUNICIPALITIES_CSV = BASE_DIR / "data" / "municipalities.csv"
RAW_DIR = BASE_DIR / "data" / "raw"
PARSED_DIR = BASE_DIR / "data" / "parsed"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LobbyAI-Survey/1.0; "
        "+https://github.com/lobbyai/local-assembly-minutes)"
    )
}
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 2.0
MAX_PAGES_PER_SITE = 30  # サイトごとの最大取得ページ数
MAX_PDF_PER_SITE = 20    # サイトごとの最大PDFダウンロード数
MAX_SESSIONS_GIJIROKU = 10   # gijiroku.com: 取得する最大会期数
MAX_SPEECHES_PER_SESSION = 20  # gijiroku.com: 1会期あたりの最大発言取得数
MAX_SCHEDULES_PER_COUNCIL = 5  # ssp.kaigiroku.net: 1会期あたりの最大スケジュール数
MAX_COUNCILS_SSP = 8  # ssp.kaigiroku.net: 取得する最大会議数

# ssp.kaigiroku.net テナント: name -> slug
SSP_SITES = {
    # 既収録（政令市）
    "さいたま市": "saitama",
    "大阪市":     "cityosaka",
    "名古屋市":   "nagoya",
    "京都市":     "kyoto",
    "相模原市":   "sagamihara",
    "浜松市":     "hamamatsu",
    "岡山市":     "okayama",
    # 中核市・一般市（追加分）
    "函館市":   "hakodate",
    "旭川市":   "asahikawa",
    "青森市":   "aomori",
    "八戸市":   "hachinohe",
    "盛岡市":   "morioka",
    "秋田市":   "akita",
    "山形市":   "yamagata",
    "郡山市":   "koriyama",
    "いわき市": "iwaki",
    "福島市":   "fukushimaCity",
    "宇都宮市": "utsunomiya",
    "前橋市":   "maebashi",
    "高崎市":   "takasaki",
    "川越市":   "kawagoe",
    "川口市":   "kawaguchicity",
    "越谷市":   "koshigaya",
    "船橋市":   "funabashi",
    "柏市":     "kashiwa",
    "横須賀市": "yokosuka",
    "藤沢市":   "fujisawa",
    "長岡市":   "nagaoka",
    "金沢市":   "kanazawa",
    "福井市":   "fukuicty",
    "甲府市":   "kofu",
    "長野市":   "nagano",
    "松本市":   "matsumoto",
    "岐阜市":   "gifucity",
    "豊橋市":   "toyohashi",
    "岡崎市":   "okazaki",
    "一宮市":   "ichinomiya",
    "豊田市":   "toyota",
    "四日市市": "yokkaichi",
    "大津市":   "otsucity",
    "豊中市":   "toyonaka",
    "高槻市":   "takatsuki",
    "枚方市":   "hirakata",
    "東大阪市": "higashiosaka",
    "吹田市":   "suita",
    "姫路市":   "himeji",
    "尼崎市":   "amagasaki",
    "西宮市":   "nishinomiya",
    "明石市":   "akashi",
    "奈良市":   "nara",
    "和歌山市": "wakayamacity",
    "鳥取市":   "tottori",
    "松江市":   "matsue",
    "倉敷市":   "kurashiki",
    "呉市":     "kure",
    "下関市":   "shimonoseki",
    "高松市":   "takamatsu",
    "松山市":   "matsuyama",
    "高知市":   "kochi",
    "久留米市": "kurume",
    "佐賀市":   "sagacity",
    "長崎市":   "nagasakicity",
    "佐世保市": "sasebo",
    "大分市":   "oitacity",
    "宮崎市":   "miyazakicity",
    "鹿児島市": "kagoshimacity",
    "那覇市":   "naha",
    "八王子市": "hachioji",
    "町田市":   "machida",
    "府中市":   "fuchuCity",
    "調布市":   "chofu",
    "西東京市": "nishitokyo",
    "水戸市":   "mitocity",
    "小山市":   "oyama",
    "所沢市":   "tokorozawa",
    "春日部市": "kasukabe",
    "上尾市":   "ageo",
    "松戸市":   "matsudo",
    "市川市":   "ichikawa",
    "平塚市":   "hiratsuka",
    "厚木市":   "atsugi",
    "八尾市":   "yaocity",
    "寝屋川市": "neyagawa",
    "加古川市": "kakogawa",
    "東広島市": "higashihiroshima",
    "沖縄市":   "okinawacity",
}

SSP_API_BASE = "https://ssp.kaigiroku.net/dnp/search"

# kensakusystem.jp テナント: name -> (tenant_path, municipality_name)
KENSAKUSYSTEM_SITES = {
    "兵庫": ("hyogopref", "兵庫"),
    "愛媛": ("ehime",     "愛媛"),
}

# gijiroku.com の自治体別CGIベースURL
# 形式: name -> (base_url, search_page_path)
GIJIROKU_SITES = {
    "栃木": ("https://pref-tochigi.gijiroku.com/voices/", "g07v_search.asp"),
    "群馬": ("https://www07.gijiroku.com/voices/", "g07v_search.asp"),
    "千葉": ("https://pref-chiba.gijiroku.com/kaigiroku/", "index.asp"),
    "石川": ("https://pref-ishikawa.gijiroku.com/voices/", "g07v_search.asp"),
    "長野": ("https://nagano.gijiroku.com/voices/", "g07v_search.asp"),
    # 政令指定都市
    "川崎市": ("https://www13.gijiroku.com/kawasaki_council/", "g07v_search.asp"),
    "新潟市": ("http://www06.gijiroku.com/niigata/", "g08v_search.asp"),
    "堺市": ("http://www12.gijiroku.com/sakai/", "g08v_search.asp"),
    "熊本市": ("http://kumamoto.gijiroku.com/voices/", "g08v_search.asp"),
}

# ナビゲーション汚染を検出するキーワード（これらが多いと本文なし判定）
NAV_NOISE_KEYWORDS = [
    "メニュー", "サイトマップ", "文字サイズ", "ホーム", "サイト内検索",
    "プライバシーポリシー", "アクセシビリティ", "ページトップ", "Cookie",
    "JavaScript", "ブラウザ", "くらし・防災", "観光・文化",
]

# 本文コンテナの候補セレクタ（先に見つかったものを優先）
MAIN_CONTENT_SELECTORS = [
    "main", "article", '[role="main"]',
    "#content", "#main", "#wrapper",
    ".content", ".main", ".article", ".post-body",
    "#contents", ".contents",
]

# 議事録ページを示すキーワード（リンクテキストや href に含まれるもの）
MINUTES_LINK_KEYWORDS = [
    "会議録", "議事録", "会議記録", "速記録",
    "本会議", "定例会", "臨時会", "委員会",
    "kaigiroku", "gijiroku", "minutes",
]

# 発言者パターン（「○○議員」「知事」「委員長」など）
SPEAKER_PATTERN = re.compile(
    r"^[　\s]*"
    r"((?:[○◯]?\s*)?(?:\S{1,20}(?:議員|知事|副知事|委員長|委員|部長|局長|課長|理事|参事|次長|主幹|係長|市長|副市長|町長|村長|議長|副議長|幹事長)))"
    r"[　\s]*$"
)

# 日付パターン
DATE_PATTERN = re.compile(
    r"(令和|平成|昭和)(\d{1,2})年\s*(\d{1,2})月\s*(\d{1,2})日"
    r"|(\d{4})[年\-/](\d{1,2})[月\-/](\d{1,2})日?"
)

# 会議名パターン
MEETING_PATTERN = re.compile(
    r"(令和|平成|昭和)\d+年\s*\d+月?\s*(?:第\d+回)?\s*"
    r"(?:定例会|臨時会|予算特別委員会|決算特別委員会|\S+委員会)"
    r"|第\d+回\s*(?:定例会|臨時会)"
)


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch(session: requests.Session, url: str) -> tuple[int, bytes]:
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        return resp.status_code, resp.content
    except Exception as e:
        return 0, b""


def extract_date(text: str) -> str:
    """テキストから日付を抽出して ISO 形式で返す。"""
    m = DATE_PATTERN.search(text)
    if not m:
        return ""
    if m.group(1):  # 元号
        era, year, month, day = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
        base = {"令和": 2018, "平成": 1988, "昭和": 1925}
        return f"{base[era] + int(year):04d}-{month:02d}-{day:02d}"
    else:
        return f"{int(m.group(5)):04d}-{int(m.group(6)):02d}-{int(m.group(7)):02d}"


def extract_meeting_name(text: str) -> str:
    m = MEETING_PATTERN.search(text)
    return m.group(0).strip() if m else ""


def is_nav_noise(text: str) -> bool:
    """ナビゲーション/ヘッダーノイズが多い場合 True を返す。"""
    if len(text) < 100:
        return True
    noise_count = sum(1 for kw in NAV_NOISE_KEYWORDS if kw in text)
    # ノイズキーワードが3つ以上 かつ テキスト長が短い場合はノイズ
    return noise_count >= 3 and len(text) < 500


def extract_main_content(soup: BeautifulSoup) -> str:
    """
    ページから本文テキストを抽出する。
    nav/header/footer/sidebar を除外し、メインコンテンツ領域を優先する。
    """
    # nav/header/footer/script/style を除去
    for tag in soup.find_all(["nav", "header", "footer", "script", "style",
                               "noscript", "aside"]):
        tag.decompose()

    # メインコンテンツ候補を探す
    for selector in MAIN_CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            text = el.get_text("\n", strip=True)
            if len(text) > 200 and not is_nav_noise(text):
                return text

    # フォールバック: body 全体
    body = soup.find("body") or soup
    return body.get_text("\n", strip=True)


def parse_minutes_html(html: str, source_url: str, municipality_name: str) -> list[dict]:
    """
    議事録HTMLを解析して発言ブロックのリストを返す。
    各要素: {municipality_name, meeting_name, meeting_date, speaker, content, source_url, fetched_at}
    """
    soup = BeautifulSoup(html, "lxml")

    # <title> から会議名・日付を取得
    title_tag = soup.find("title")
    title_text = title_tag.get_text(" ", strip=True) if title_tag else ""
    # h1/h2 も参照
    for h in soup.find_all(["h1", "h2"], limit=3):
        title_text += " " + h.get_text(" ", strip=True)

    meeting_name = extract_meeting_name(title_text)
    meeting_date = extract_date(title_text)

    # 本文抽出（ノイズ除去済み）
    raw_text = extract_main_content(soup)

    # ノイズしかない場合はスキップ
    if is_nav_noise(raw_text):
        return []

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]

    records = []
    current_speaker = ""
    buffer = []

    def flush(speaker, buf):
        content = "\n".join(buf).strip()
        if content and len(content) > 30:
            records.append({
                "municipality_name": municipality_name,
                "meeting_name": meeting_name,
                "meeting_date": meeting_date,
                "speaker": speaker,
                "content": content[:4000],  # 1レコードの上限
                "source_url": source_url,
                "fetched_at": datetime.utcnow().isoformat(),
            })

    for line in lines:
        m = SPEAKER_PATTERN.match(line)
        if m:
            flush(current_speaker, buffer)
            current_speaker = m.group(1)
            buffer = []
        else:
            buffer.append(line)

    flush(current_speaker, buffer)

    # 発言ブロックが少ない場合は全文を1レコードとして格納
    if not records and raw_text:
        records.append({
            "municipality_name": municipality_name,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
            "speaker": "",
            "content": raw_text[:8000],
            "source_url": source_url,
            "fetched_at": datetime.utcnow().isoformat(),
        })

    return records


def find_minutes_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """トップページから議事録ページへのリンクを収集する。"""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        # キーワードマッチ
        matched = any(kw in text or kw in href for kw in MINUTES_LINK_KEYWORDS)
        if not matched:
            continue
        abs_url = urllib.parse.urljoin(base_url, href)
        # 外部ドメインは除外（同一ドメインのみ辿る）
        if urllib.parse.urlparse(abs_url).netloc != urllib.parse.urlparse(base_url).netloc:
            continue
        if abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)
    return links


def find_pdf_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """ページから PDF リンクを収集する。"""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if href.lower().endswith(".pdf"):
            abs_url = urllib.parse.urljoin(base_url, href)
            if abs_url not in seen:
                seen.add(abs_url)
                links.append(abs_url)
    return links


def scrape_static(session, name: str, minutes_url: str) -> tuple[list[dict], list[str]]:
    """
    静的HTML議事録サイトを巡回してレコードとPDFリンクを返す。
    返り値: (html_records, pdf_urls)
    """
    print(f"  [静的HTML] {minutes_url}")
    status, content = fetch(session, minutes_url)
    if status != 200:
        print(f"  -> HTTP {status}、スキップ")
        return [], []

    soup = BeautifulSoup(content, "lxml")
    child_links = find_minutes_links(soup, minutes_url)
    all_records = []
    all_pdf_urls: list[str] = []

    # トップページ自体に議事録テキストがあれば取得
    top_records = parse_minutes_html(content.decode("utf-8", errors="replace"), minutes_url, name)
    all_records.extend(top_records)
    all_pdf_urls.extend(find_pdf_links(soup, minutes_url))

    # 子リンクを巡回（3階層まで）
    visited = {minutes_url}
    queue = [(link, 1) for link in child_links[:MAX_PAGES_PER_SITE]]

    while queue and len(visited) < MAX_PAGES_PER_SITE + 1:
        link, depth = queue.pop(0)
        if link in visited:
            continue
        visited.add(link)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        status2, content2 = fetch(session, link)
        if status2 != 200:
            continue

        html2 = content2.decode("utf-8", errors="replace")
        records = parse_minutes_html(html2, link, name)
        all_records.extend(records)

        soup2 = BeautifulSoup(content2, "lxml")
        # このページのPDFリンクを収集
        all_pdf_urls.extend(find_pdf_links(soup2, link))

        # 深さ3まで再帰
        if depth < 3:
            deeper = find_minutes_links(soup2, link)
            for dlink in deeper[:8]:
                if dlink not in visited:
                    queue.append((dlink, depth + 1))

    # PDF URLの重複除去
    all_pdf_urls = list(dict.fromkeys(all_pdf_urls))

    if all_pdf_urls:
        print(f"  -> PDFリンク {len(all_pdf_urls)} 件を発見")

    return all_records, all_pdf_urls


def _download_pdfs(session, name: str, pdf_urls: list[str], out_dir: Path) -> int:
    """PDF URLリストをダウンロードして件数を返す（内部共通処理）。"""
    count = 0
    for pdf_url in pdf_urls:
        fname = re.sub(r"[^\w\-.]", "_", urllib.parse.urlparse(pdf_url).path.split("/")[-1])
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        out_path = out_dir / fname
        if out_path.exists():
            continue
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        s, c = fetch(session, pdf_url)
        if s == 200 and c:
            out_path.write_bytes(c)
            meta_path = out_path.with_suffix(".meta.json")
            meta_path.write_text(json.dumps({
                "municipality_name": name,
                "source_url": pdf_url,
                "fetched_at": datetime.utcnow().isoformat(),
            }, ensure_ascii=False), encoding="utf-8")
            print(f"    DL: {fname} ({len(c):,} bytes)")
            count += 1
    return count


def scrape_pdf(session, name: str, minutes_url: str, out_dir: Path) -> int:
    """PDFリンクを収集してダウンロード。ダウンロード件数を返す。"""
    print(f"  [PDF] {minutes_url}")
    status, content = fetch(session, minutes_url)
    if status != 200:
        print(f"  -> HTTP {status}、スキップ")
        return 0

    soup = BeautifulSoup(content, "lxml")
    pdf_links = find_pdf_links(soup, minutes_url)

    # リンクが少ない場合、子ページも探索
    if len(pdf_links) < 3:
        child_links = find_minutes_links(soup, minutes_url)
        for clink in child_links[:5]:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            s2, c2 = fetch(session, clink)
            if s2 == 200:
                s2_soup = BeautifulSoup(c2, "lxml")
                pdf_links.extend(find_pdf_links(s2_soup, clink))

    # 重複除去
    pdf_links = list(dict.fromkeys(pdf_links))[:MAX_PDF_PER_SITE]
    count = 0
    for pdf_url in pdf_links:
        fname = re.sub(r"[^\w\-.]", "_", urllib.parse.urlparse(pdf_url).path.split("/")[-1])
        out_path = out_dir / fname
        if out_path.exists():
            print(f"    スキップ（既存）: {fname}")
            continue
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        s, c = fetch(session, pdf_url)
        if s == 200 and c:
            out_path.write_bytes(c)
            # メタ情報も保存
            meta_path = out_path.with_suffix(".meta.json")
            meta_path.write_text(json.dumps({
                "municipality_name": name,
                "source_url": pdf_url,
                "fetched_at": datetime.utcnow().isoformat(),
            }, ensure_ascii=False), encoding="utf-8")
            print(f"    DL: {fname} ({len(c):,} bytes)")
            count += 1
    return count


def _gijiroku_detect_cgi(session, base_url: str, search_page: str) -> str:
    """
    gijiroku.com のCGIベースURLを検出する。
    search_page から iframe src を読んで cgi/voiweb.exe のパスを特定する。
    """
    search_url = base_url + search_page
    status, content = fetch(session, search_url)
    if status != 200:
        return base_url + "cgi/voiweb.exe"  # デフォルトパス

    try:
        text = content.decode("shift_jis", errors="replace")
    except Exception:
        text = content.decode("utf-8", errors="replace")

    soup = BeautifulSoup(text, "lxml")
    iframe = soup.find("iframe", src=True)
    if iframe:
        src = iframe["src"]
        # "cgi/voiweb.exe?..." の形式から CGIパスを取得
        cgi_rel = src.split("?")[0]  # "cgi/voiweb.exe"
        cgi_url = urllib.parse.urljoin(search_url, cgi_rel)
        return cgi_url

    return base_url + "cgi/voiweb.exe"


def _gijiroku_fetch_sjis(session, url: str):
    """Shift-JIS ページを取得してデコードした文字列を返す。"""
    status, content = fetch(session, url)
    if status != 200:
        return ""
    try:
        return content.decode("shift_jis", errors="replace")
    except Exception:
        return content.decode("utf-8", errors="replace")


def _gijiroku_extract_date_from_parts(meeting_name: str, session_num: str) -> str:
    """
    gijiroku.com の会議名（令和X年含む）とセッション番号（月日含む）から日付を取得。
    例: meeting_name="令和７年第３回定例会..." session_num="12月12日-01号" → "2025-12-12"
    """
    era_m = re.search(r"(令和|平成|昭和)\s*(\d{1,2})\s*年", meeting_name)
    if not era_m:
        return extract_date(meeting_name + " " + session_num)
    era, year = era_m.group(1), int(era_m.group(2))
    base = {"令和": 2018, "平成": 1988, "昭和": 1925}
    year_ad = base[era] + year
    date_m = re.search(r"(\d{1,2})月(\d{1,2})日", session_num)
    if not date_m:
        return f"{year_ad:04d}"
    month, day = int(date_m.group(1)), int(date_m.group(2))
    return f"{year_ad:04d}-{month:02d}-{day:02d}"


def _gijiroku_extract_session_list(cgi_url: str, text: str) -> list[dict]:
    """
    ACT=100 結果ページから会期リスト（onclick の ACT=200 URL）を抽出する。
    返り値: [{"act200_url": ..., "meeting_name": ..., "meeting_date": ...}, ...]
    """
    cgi_base = cgi_url.rsplit("/voiweb.exe", 1)[0] + "/"
    sessions = []
    soup = BeautifulSoup(text, "lxml")
    seen_urls = set()

    for a in soup.find_all("a", onclick=True):
        onclick = a.get("onclick", "")
        m = re.search(r"winopen\('(voiweb\.exe\?ACT=200[^']+)'\)", onclick)
        if not m:
            continue
        act200_path = m.group(1)
        act200_url = cgi_base + act200_path
        if act200_url in seen_urls:
            continue
        seen_urls.add(act200_url)

        # 会期情報はテーブルセルに "令和X年...,月日-号" 形式で入っている
        td = a.find_parent("td")
        meeting_info = td.get_text(" ", strip=True) if td else a.get_text(strip=True)
        # "令和X年第Y回定例会, 12月12日-01号" → 会議名と日付を分離
        if "," in meeting_info:
            meeting_name_raw, session_num = meeting_info.rsplit(",", 1)
        else:
            meeting_name_raw = meeting_info
            session_num = ""

        meeting_name = re.sub(r"\s+", " ", meeting_name_raw).strip()
        meeting_date = _gijiroku_extract_date_from_parts(meeting_name, session_num)

        sessions.append({
            "act200_url": act200_url,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
        })

    return sessions


def _gijiroku_parse_fullpage_speeches(text203: str, act200_url: str,
                                       meeting_name: str, meeting_date: str,
                                       municipality_name: str) -> list[dict]:
    """
    「全文モード」の ACT=203 ページ（全発言が1ページにある）から
    HUID アンカーで区切って発言レコードを返す。
    """
    soup203 = BeautifulSoup(text203, "lxml")
    for s in soup203.find_all("script"):
        s.decompose()

    # HUID アンカー位置で HTML を分割してテキストを抽出
    huid_anchors = soup203.find_all("a", attrs={"name": re.compile(r"^HUID\d+$", re.IGNORECASE)})
    records = []
    fetched_at = datetime.utcnow().isoformat()

    for i, anchor in enumerate(huid_anchors):
        huid = re.sub(r"(?i)huid", "", anchor.get("name", ""))

        # このアンカーから次のアンカーまでの兄弟要素テキストを収集
        texts = []
        el = anchor.next_sibling
        while el:
            # 次の HUID アンカーに到達したら停止
            if hasattr(el, "get") and re.match(r"(?i)^HUID\d+$", el.get("name", "")):
                break
            if hasattr(el, "get_text"):
                t = el.get_text(strip=True)
                if t:
                    texts.append(t)
            elif isinstance(el, str) and el.strip():
                texts.append(el.strip())
            el = el.next_sibling

        content = "\n".join(texts).strip()
        if len(content) < 30:
            continue

        # 先頭行から発言者を推定（"○委員長(白石)" や "◎部長名" など）
        first_line = content.split("\n")[0]
        speaker_m = re.match(r"^[○◯◎◆●]\s*(.{1,30})", first_line)
        speaker = speaker_m.group(1).strip() if speaker_m else ""

        records.append({
            "municipality_name": municipality_name,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
            "speaker": speaker,
            "content": content[:5000],
            "source_url": act200_url + f"#HUID{huid}",
            "fetched_at": fetched_at,
        })

    return records


def _gijiroku_get_speeches(session, cgi_url: str, act200_url: str,
                            meeting_name: str, meeting_date: str,
                            municipality_name: str) -> list[dict]:
    """
    1会期の発言ブロックを取得してレコードリストを返す。
    voiweb.exe のバージョンにより2モードを自動検出:
    - 全文モード（新版）: ACT=203 1リクエストで全発言取得
    - per-HUID モード（旧版）: ACT=202 サイドバーから HUID 取得 → 個別リクエスト
    """
    cgi_base = cgi_url.rsplit("/voiweb.exe", 1)[0] + "/"
    records = []

    # ACT=200: フレームセット取得
    text200 = _gijiroku_fetch_sjis(session, act200_url)
    if not text200:
        return []

    # フレームタイトル（会議名・日付）を補完
    title_m = re.search(r"<TITLE>([^<]+)</TITLE>", text200, re.IGNORECASE)
    if title_m and not meeting_name:
        raw_title = title_m.group(1)
        if "−" in raw_title:
            meeting_name = raw_title.split("−")[0].strip()

    # ACT=203 ベースURL（本文フレーム）
    m203 = re.search(r'SRC="(voiweb\.exe\?ACT=203[^"]+)"', text200)
    if not m203:
        return []
    act203_base = cgi_base + m203.group(1).replace("&amp;", "&").split("#")[0]

    time.sleep(SLEEP_BETWEEN_REQUESTS)

    # まず ACT=203 (HUID なし) を取得してモードを判定
    text203_full = _gijiroku_fetch_sjis(session, act203_base)
    huid_anchors_full = re.findall(
        r'<[Aa]\s+[Nn][Aa][Mm][Ee]="HUID\d+"', text203_full
    )

    if len(huid_anchors_full) > 1:
        # 全文モード: 1ページに全発言が含まれている
        records = _gijiroku_parse_fullpage_speeches(
            text203_full, act200_url, meeting_name, meeting_date, municipality_name
        )
        return records[:MAX_SPEECHES_PER_SESSION]

    # per-HUID モード: ACT=202 サイドバーから HUID を収集
    m202 = re.search(r'SRC="(voiweb\.exe\?ACT=202[^"]+)"', text200)
    if not m202:
        return []
    act202_url = cgi_base + m202.group(1).replace("&amp;", "&")

    time.sleep(SLEEP_BETWEEN_REQUESTS)
    text202 = _gijiroku_fetch_sjis(session, act202_url)
    if not text202:
        return []

    soup202 = BeautifulSoup(text202, "lxml")
    speech_entries = []  # [(huid, speaker_name), ...]
    seen_huids = set()

    # パターン1: href に HUID={N} を含む（群馬型）
    for a in soup202.find_all("a", href=True):
        href = a.get("href", "")
        huid_m = re.search(r"HUID=(\d+)", href)
        if not huid_m:
            continue
        huid = huid_m.group(1)
        if huid in seen_huids:
            continue
        seen_huids.add(huid)
        speaker = a.get_text(strip=True)
        if speaker.startswith("(") and speaker.endswith(")"):
            continue
        speech_entries.append((huid, speaker))

    # パターン2: <A NAME="{N}"> 方式（長野・石川型）
    if not speech_entries:
        for anchor in soup202.find_all("a", attrs={"name": re.compile(r"^\d{5,7}$")}):
            huid = anchor["name"]
            if huid in seen_huids:
                continue
            seen_huids.add(huid)
            # 同じ TABLE 内の最後の TD のテキストが発言者名
            table = anchor.find_parent("table")
            speaker = ""
            if table:
                tds = table.find_all("td")
                # リンクを含む TD を発言者名として取得
                for td in reversed(tds):
                    a_in_td = td.find("a", href=True)
                    if a_in_td:
                        t = a_in_td.get_text(strip=True)
                        if t and not (t.startswith("(") and t.endswith(")")):
                            speaker = t
                            break
            speech_entries.append((huid, speaker))

    speech_entries = speech_entries[:MAX_SPEECHES_PER_SESSION]

    for huid, speaker in speech_entries:
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        act203_url = act203_base + f"&HUID={huid}"
        text203 = _gijiroku_fetch_sjis(session, act203_url)
        if not text203:
            continue

        soup203 = BeautifulSoup(text203, "lxml")
        for script in soup203.find_all("script"):
            script.decompose()
        body = soup203.find("body")
        if not body:
            continue
        content = body.get_text("\n", strip=True)

        if len(content) < 30:
            continue

        records.append({
            "municipality_name": municipality_name,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
            "speaker": speaker,
            "content": content[:5000],
            "source_url": act200_url + f"#HUID{huid}",
            "fetched_at": datetime.utcnow().isoformat(),
        })

    return records


def scrape_gijiroku(session, name: str, minutes_url: str) -> list[dict]:
    """
    gijiroku.com 系サイトをスクレイピングして発言レコードを返す。
    直接 voiweb.exe CGI にアクセスして Shift-JIS エンコード処理を行う。
    """
    # CGI ベースURL を検出
    if name in GIJIROKU_SITES:
        base_url, search_page = GIJIROKU_SITES[name]
    else:
        # URL から推測
        parsed = urllib.parse.urlparse(minutes_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}/"
        search_page = "voices/g07v_search.asp"

    print(f"  [gijiroku.com] {base_url}")

    cgi_url = _gijiroku_detect_cgi(session, base_url, search_page)
    print(f"  CGI: {cgi_url}")

    # ACT=100 (キーワードなし) で最新会期一覧を取得
    act100_url = (
        cgi_url + "?ACT=100&KENSAKU=0&SORT=0"
        "&KTYP=1,2,3,0&KGTP=1,2,3"
    )
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    text100 = _gijiroku_fetch_sjis(session, act100_url)
    if not text100:
        print("  -> ACT=100 取得失敗")
        return []

    hit_m = re.search(r"(\d+)件の日程がヒットしました", text100)
    total_sessions = int(hit_m.group(1)) if hit_m else 0
    sessions = _gijiroku_extract_session_list(cgi_url, text100)
    sessions = sessions[:MAX_SESSIONS_GIJIROKU]
    print(f"  -> 総会期数: {total_sessions} / 取得対象: {len(sessions)} 件")

    all_records = []

    for i, s in enumerate(sessions, start=1):
        print(f"    [{i:02d}/{len(sessions):02d}] {s['meeting_name'][:40]} {s['meeting_date']}")
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        records = _gijiroku_get_speeches(
            session, cgi_url,
            s["act200_url"], s["meeting_name"], s["meeting_date"],
            name,
        )
        print(f"      -> {len(records)} 件の発言を取得")
        all_records.extend(records)

    return all_records


def _ssp_post(session, endpoint: str, data: dict) -> dict:
    """ssp.kaigiroku.net の REST API に POST する。"""
    url = f"{SSP_API_BASE}/{endpoint}"
    try:
        resp = session.post(
            url, data=data,
            headers={"Referer": "https://ssp.kaigiroku.net/tenant/saitama/SpTop.html"},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def scrape_ssp_kaigiroku(session, name: str, slug: str) -> list[dict]:
    """
    ssp.kaigiroku.net テナントをスクレイピングして発言レコードを返す。
    REST API: /dnp/search/councils/index → /minutes/get_schedule → /minutes/get_minute
    """
    print(f"  [ssp.kaigiroku.net] tenant={slug}")

    # tenant_id を取得
    tid_url = f"https://ssp.kaigiroku.net/tenant/{slug}/js/tenant.js"
    _, content = fetch(session, tid_url)
    tid_text = content.decode("utf-8", errors="replace")
    tid_m = re.search(r"tenant_id\s*=\s*(\d+)", tid_text)
    if not tid_m:
        print("  -> tenant_id 取得失敗")
        return []
    tenant_id = int(tid_m.group(1))
    print(f"  tenant_id={tenant_id}")

    # 会議一覧を取得
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    councils_data = _ssp_post(session, "councils/index", {"tenant_id": tenant_id})
    councils = []
    for c in councils_data.get("councils", []):
        for vy in c.get("view_years", []):
            for ct in vy.get("council_type", []):
                for cc in ct.get("councils", []):
                    councils.append({
                        "council_id": cc["council_id"],
                        "name": cc["name"].strip(),
                        "view_year": vy["view_year"],
                    })

    councils = councils[:MAX_COUNCILS_SSP]
    print(f"  -> 会議数: {len(councils)} 件を処理")

    all_records = []
    fetched_at = datetime.utcnow().isoformat()

    for council in councils:
        council_id = council["council_id"]
        meeting_name = council["name"]
        meeting_date = extract_date(meeting_name)
        # 会議名に日が含まれない場合（例: "令和７年１２月定例会"）は view_year + 月で補完
        if not meeting_date:
            yr = int(council["view_year"]) if council["view_year"].isdigit() else 0
            month_m = re.search(r"(\d{1,2})\s*月", meeting_name)
            if yr and month_m:
                meeting_date = f"{yr}-{int(month_m.group(1)):02d}-01"

        time.sleep(SLEEP_BETWEEN_REQUESTS)
        sched_data = _ssp_post(
            session, "minutes/get_schedule",
            {"tenant_id": tenant_id, "council_id": council_id},
        )
        schedules = sched_data.get("council_schedules", [])

        for sched in schedules[:MAX_SCHEDULES_PER_COUNCIL]:
            schedule_id = sched["schedule_id"]
            sched_name = sched.get("name", "")
            if not meeting_date:
                meeting_date = extract_date(sched_name)
            # スケジュール名（例: "11月26日－01号"）から日付補完
            if meeting_date and meeting_date.endswith("-01"):
                sched_date_m = re.search(r"(\d{1,2})月(\d{1,2})日", sched_name)
                if sched_date_m:
                    year = meeting_date[:4]
                    month = int(sched_date_m.group(1))
                    day = int(sched_date_m.group(2))
                    meeting_date = f"{year}-{month:02d}-{day:02d}"

            time.sleep(SLEEP_BETWEEN_REQUESTS)
            min_data = _ssp_post(
                session, "minutes/get_minute",
                {"tenant_id": tenant_id, "council_id": council_id, "schedule_id": schedule_id},
            )
            minutes_list = min_data.get("tenant_minutes", [])

            source_base = (
                f"https://ssp.kaigiroku.net/tenant/{slug}/SpMinuteView.html"
                f"?council_id={council_id}&schedule_id={schedule_id}"
            )

            for m in minutes_list:
                if m.get("minute_type_code") == 2:  # 名簿 (member list) をスキップ
                    continue
                body_raw = m.get("body", "")
                body = re.sub(r"<[^>]+>", "", body_raw).strip()
                if len(body) < 30:
                    continue
                title = m.get("title", "").strip()
                speaker = re.sub(r"^[○◯△◎◆●]\s*", "", title)

                # minute_id を使ってユニークな source_url を生成
                minute_id = m.get("minute_id", "")
                source_url = source_base + f"&minute_id={minute_id}"

                all_records.append({
                    "municipality_name": name,
                    "meeting_name": meeting_name,
                    "meeting_date": meeting_date,
                    "speaker": speaker,
                    "content": body[:5000],
                    "source_url": source_url,
                    "fetched_at": fetched_at,
                })

        print(f"    {meeting_name}: {len(all_records)} 件累計")

    return all_records


def _kensakusystem_get_code(session, tenant_path: str) -> str:
    """kensakusystem.jp のトップページから Code セッショントークンを取得する。"""
    url = f"https://www.kensakusystem.jp/{tenant_path}/"
    _, content = fetch(session, url)
    text = content.decode("shift_jis", errors="replace")
    m = re.search(r"Code=([a-z0-9]+)", text)
    return m.group(1) if m else ""


def scrape_kensakusystem(session, name: str, tenant_path: str) -> list[dict]:
    """
    kensakusystem.jp をスクレイピングして発言レコードを返す。
    Search2.exe → data-context → GetText3.exe の流れ。
    """
    print(f"  [kensakusystem.jp] tenant={tenant_path}")

    code = _kensakusystem_get_code(session, tenant_path)
    if not code:
        print("  -> Code 取得失敗")
        return []
    print(f"  Code={code}")

    # 広義キーワード「議員」で検索 → 各会期の代表的な発言を収集
    kw = "議員"
    sjis_kw = urllib.parse.quote(kw.encode("shift_jis"))
    sjis_search = urllib.parse.quote("検索".encode("shift_jis"))

    search_url = (
        f"https://www.kensakusystem.jp/{tenant_path}/cgi-bin3/Search2.exe"
        f"?Code={code}&dMode=0&KeyWord={sjis_kw}&searchMode=3&keyMode=10"
        f"&alltarget=on&eTarget=1&sTarget=2&AhitResult={sjis_search}"
    )
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    status, content = fetch(session, search_url)
    if status != 200:
        print(f"  -> Search2.exe HTTP {status}")
        return []

    text = content.decode("shift_jis", errors="replace")
    soup = BeautifulSoup(text, "lxml")

    # data-context 属性から結果を取得
    result_links = soup.find_all("a", {"data-context": True})
    print(f"  -> 検索結果: {len(result_links)} 件")

    # 各結果から (file_id, pos) のリストを作成（重複除去しつつ全件収集）
    seen_contexts: set[str] = set()
    items: list[dict] = []
    for link in result_links:
        ctx = link.get("data-context", "")
        if ctx in seen_contexts:
            continue
        seen_contexts.add(ctx)
        parts = ctx.split("/")
        if len(parts) < 3:
            continue
        file_id, pos, session_id = parts[0], parts[1], parts[2]

        # 議題行（pos=262 など）はスキップ（会議情報を含む行は別途処理）
        tr = link.find_parent("tr")
        meeting_info = tr.get_text(" ", strip=True) if tr else ""
        meeting_name_m = re.search(
            r"(令和|平成|昭和)\s*\d+\s*年\s*\d+\s*月[^\s]{0,30}(?:定例会|臨時会|特別委員会|委員会)?",
            meeting_info
        )
        meeting_name = meeting_name_m.group(0).strip() if meeting_name_m else ""
        meeting_date = extract_date(meeting_info)

        items.append({
            "file_id": file_id,
            "pos": pos,
            "session_id": session_id,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
        })

    items = items[:MAX_SESSIONS_GIJIROKU * MAX_SPEECHES_PER_SESSION]
    print(f"  -> 取得対象: {len(items)} 件")

    all_records = []
    fetched_at = datetime.utcnow().isoformat()
    base_url = f"https://www.kensakusystem.jp/{tenant_path}"

    for s in items:
        file_id = s["file_id"]
        pos = s["pos"]
        get_url = (
            f"{base_url}/cgi-bin3/GetText3.exe"
            f"?{code}/{file_id}/{pos}/10/3//1/{urllib.parse.quote(kw)}/0"
        )
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        status, content = fetch(session, get_url)
        if status != 200:
            continue

        html = content.decode("shift_jis", errors="replace")
        soup3 = BeautifulSoup(html, "lxml")

        # タイトルから発言者を取得
        title_tag = soup3.find("title")
        title_text = title_tag.get_text(strip=True) if title_tag else ""
        # "令和X年Y月定例会（第N日 M月D日） 発言者名" 形式
        speaker = ""
        if "　" in title_text:
            speaker = title_text.split("　")[-1].strip()
        elif " " in title_text:
            speaker = title_text.split(" ")[-1].strip()

        # 本文テキストを抽出
        for tag in soup3.find_all(["script", "style"]):
            tag.decompose()
        body = soup3.get_text("\n", strip=True)
        if len(body) < 50:
            continue

        # 会議名/日付が未取得の場合はタイトルから補完
        meeting_name = s["meeting_name"] or extract_meeting_name(title_text)
        meeting_date = s["meeting_date"] or extract_date(title_text)

        source_url = (
            f"{base_url}/cgi-bin3/ResultFrame.exe"
            f"?Code={code}&STYPE={s['session_id']}&MODE=0"
        )

        all_records.append({
            "municipality_name": name,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
            "speaker": speaker,
            "content": body[:5000],
            "source_url": source_url,
            "fetched_at": fetched_at,
        })

    return all_records


def load_survey() -> list[dict]:
    """survey_report.csv を読み込む。存在しない場合は municipalities.csv から全件返す。"""
    if SURVEY_CSV.exists():
        with open(SURVEY_CSV, encoding="utf-8") as f:
            return list(csv.DictReader(f))

    print(f"警告: {SURVEY_CSV} が見つかりません。municipalities.csv から全件読み込みます。", file=sys.stderr)
    with open(MUNICIPALITIES_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # survey 形式に変換（difficulty=A とみなす）
    return [
        {
            "name": r["name"],
            "assembly_url": r["assembly_url"],
            "minutes_url": r["minutes_url"],
            "access_type": "static",
            "robots_txt_ok": "unknown",
            "structure_notes": "",
            "difficulty": "A",
        }
        for r in rows
    ]


def main():
    rows = load_survey()

    # A → B → C の順でソート
    targets = [r for r in rows if r.get("difficulty") in ("A", "B", "C")]
    targets.sort(key=lambda r: r.get("difficulty", "Z"))

    if not targets:
        print("difficulty=A/B/C の自治体が見つかりません。")
        sys.exit(0)

    cnt = {d: sum(1 for r in targets if r["difficulty"] == d) for d in ("A", "B", "C")}
    print(f"対象: {len(targets)} 件（A={cnt['A']}, B={cnt['B']}, C={cnt['C']}）")
    print("=" * 60)

    session = get_session()
    total_records = 0
    total_pdfs = 0

    for i, row in enumerate(targets, start=1):
        name = row["name"]
        difficulty = row.get("difficulty", "A")
        minutes_url = row.get("minutes_url") or row.get("assembly_url", "")

        print(f"\n[{i:02d}/{len(targets):02d}] {name} (難易度:{difficulty})")

        if not minutes_url:
            print("  -> URL なし、スキップ")
            continue

        try:
            if difficulty == "A":
                parsed_dir = PARSED_DIR / name
                parsed_dir.mkdir(parents=True, exist_ok=True)
                records, pdf_urls = scrape_static(session, name, minutes_url)

                # HTMLで取れたレコードを保存
                if records:
                    out_file = parsed_dir / "minutes.json"
                    with open(out_file, "w", encoding="utf-8") as f:
                        json.dump(records, f, ensure_ascii=False, indent=2)
                    print(f"  -> HTML: {len(records)} レコード保存")
                    total_records += len(records)
                else:
                    print("  -> HTMLレコードなし")

                # 巡回中に見つかったPDFもダウンロード
                if pdf_urls:
                    pdf_dir = RAW_DIR / name
                    pdf_dir.mkdir(parents=True, exist_ok=True)
                    count = _download_pdfs(session, name, pdf_urls[:MAX_PDF_PER_SITE], pdf_dir)
                    print(f"  -> PDF: {count} 件ダウンロード")
                    total_pdfs += count

            elif difficulty == "B":
                out_dir = RAW_DIR / name
                out_dir.mkdir(parents=True, exist_ok=True)
                count = scrape_pdf(session, name, minutes_url, out_dir)
                print(f"  -> {count} 件の PDF ダウンロード完了")
                total_pdfs += count

            elif difficulty == "C":
                # 外部検索システム: gijiroku.com / ssp.kaigiroku.net / kensakusystem.jp
                records = []
                if "gijiroku.com" in minutes_url or name in GIJIROKU_SITES:
                    records = scrape_gijiroku(session, name, minutes_url)
                elif "ssp.kaigiroku.net" in minutes_url or name in SSP_SITES:
                    slug = SSP_SITES.get(name)
                    if not slug:
                        # URL から slug を推測: /tenant/{slug}/SpTop.html
                        m = re.search(r"/tenant/([^/]+)/", minutes_url)
                        slug = m.group(1) if m else None
                    if slug:
                        records = scrape_ssp_kaigiroku(session, name, slug)
                    else:
                        print(f"  -> SSP slug 不明: {minutes_url}")
                elif "kensakusystem.jp" in minutes_url or name in KENSAKUSYSTEM_SITES:
                    tenant_path = KENSAKUSYSTEM_SITES.get(name, (None,))[0]
                    if not tenant_path:
                        # URL から tenant_path を推測
                        m = re.search(r"kensakusystem\.jp/([^/]+)/", minutes_url)
                        tenant_path = m.group(1) if m else None
                    if tenant_path:
                        records = scrape_kensakusystem(session, name, tenant_path)
                    else:
                        print(f"  -> kensakusystem.jp テナント不明: {minutes_url}")
                else:
                    print(f"  -> 未対応の difficulty=C システム: {minutes_url}")

                if records:
                    parsed_dir = PARSED_DIR / name
                    parsed_dir.mkdir(parents=True, exist_ok=True)
                    out_file = parsed_dir / "minutes.json"
                    # 既存レコードとマージ（source_url で重複除外）
                    existing = []
                    if out_file.exists():
                        try:
                            existing = json.loads(out_file.read_text(encoding="utf-8"))
                        except Exception:
                            existing = []
                    existing_urls = {r.get("source_url") for r in existing}
                    new_records = [r for r in records
                                   if r.get("source_url") not in existing_urls]
                    all_records_c = existing + new_records
                    with open(out_file, "w", encoding="utf-8") as f:
                        json.dump(all_records_c, f, ensure_ascii=False, indent=2)
                    print(f"  -> {len(new_records)} 件保存（合計 {len(all_records_c)} 件）")
                    total_records += len(new_records)
                elif difficulty == "C" and not records:
                    print("  -> レコードなし")

        except Exception as e:
            print(f"  -> エラー（スキップ）: {e}")
            continue

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print("\n" + "=" * 60)
    print(f"完了: HTMLレコード {total_records} 件 / PDF {total_pdfs} 件")
    print(f"次のステップ: python 03_pdf_parser.py  # PDFをテキスト化")
    print(f"             python 04_indexer.py       # DBに格納")
    print(f"             （difficulty=C の gijiroku.com レコードは直接 parsed/ に保存済み）")


if __name__ == "__main__":
    main()
