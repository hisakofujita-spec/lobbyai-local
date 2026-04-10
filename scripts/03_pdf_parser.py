#!/usr/bin/env python3
"""
03_pdf_parser.py - PDF議事録テキスト抽出

data/raw/*/*.pdf を pdfplumber で読み込み、発言ブロックに分割して
data/parsed/*/minutes.json に追記する。
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

try:
    import pdfplumber
except ImportError:
    print("pdfplumber が未インストールです: pip install pdfplumber", file=sys.stderr)
    sys.exit(1)

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PARSED_DIR = BASE_DIR / "data" / "parsed"

# 発言者パターン（議事録の典型的な形式）
SPEAKER_PATTERNS = [
    # ○田中議員、◯知事、○委員長 など
    re.compile(r"^[○◯]\s*(.{1,20}?(?:議員|知事|副知事|委員長|委員|部長|局長|課長|市長|副市長|町長|村長|議長|副議長|幹事長|理事|参事))\s*$"),
    # 「田中議員」（括弧なし、行頭）
    re.compile(r"^(\S{1,10}(?:議員|知事|副知事|委員長|委員|部長|局長|課長|市長|副市長|町長|村長|議長|副議長))\s*$"),
]

DATE_PATTERN = re.compile(
    r"(令和|平成|昭和)(\d{1,2})年\s*(\d{1,2})月\s*(\d{1,2})日"
    r"|(\d{4})[年\-/](\d{1,2})[月\-/](\d{1,2})日?"
)

MEETING_PATTERN = re.compile(
    r"(令和|平成|昭和)\d+年\s*\d+月?\s*(?:第\d+回)?\s*"
    r"(?:定例会|臨時会|予算特別委員会|決算特別委員会|\S+委員会)"
    r"|第\d+回\s*(?:定例会|臨時会)"
)


def era_to_iso(era: str, year: int, month: int, day: int) -> str:
    base = {"令和": 2018, "平成": 1988, "昭和": 1925}
    return f"{base[era] + year:04d}-{month:02d}-{day:02d}"


def extract_date(text: str) -> str:
    m = DATE_PATTERN.search(text)
    if not m:
        return ""
    if m.group(1):
        return era_to_iso(m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4)))
    return f"{int(m.group(5)):04d}-{int(m.group(6)):02d}-{int(m.group(7)):02d}"


def extract_meeting_name(text: str) -> str:
    m = MEETING_PATTERN.search(text)
    return m.group(0).strip() if m else ""


def match_speaker(line: str) -> Optional[str]:
    """行が発言者行なら発言者名を返す。"""
    line = line.strip()
    for pat in SPEAKER_PATTERNS:
        m = pat.match(line)
        if m:
            return m.group(1).strip()
    return None


def clean_pdf_text(text: str) -> str:
    """
    PDF抽出テキストの典型的な問題を修正する。
    - 全角文字間の余分なスペースを除去（例: 「産 後 ケ ア」→「産後ケア」）
    - 行末ハイフンによる分割を修復
    """
    # CJK文字・ひらがな・カタカナ・全角記号の範囲
    cjk = r'[\u3000-\u9fff\uff00-\uffef\u3040-\u309f\u30a0-\u30ff]'
    # CJK文字間の半角スペースを除去
    text = re.sub(rf'({cjk})\s+({cjk})', r'\1\2', text)
    # 繰り返し適用（3文字以上連続スペースがある場合）
    text = re.sub(rf'({cjk})\s+({cjk})', r'\1\2', text)
    return text


def extract_text_from_pdf(pdf_path: Path) -> str:
    """pdfplumber でPDFの全テキストを抽出する。"""
    texts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            if text:
                texts.append(clean_pdf_text(text))
    return "\n".join(texts)


def split_into_speeches(text: str, municipality_name: str, meeting_name: str,
                         meeting_date: str, source_url: str) -> list[dict]:
    """
    テキストを発言ブロックに分割してレコードリストを返す。
    発言者が検出できない場合は段落ごとに分割する。
    """
    lines = [ln.strip() for ln in text.splitlines()]
    records = []
    current_speaker = ""
    buffer = []

    def flush(speaker: str, buf: list[str]):
        content = "\n".join(buf).strip()
        # 短すぎるブロックは無視
        if len(content) < 20:
            return
        records.append({
            "municipality_name": municipality_name,
            "meeting_name": meeting_name,
            "meeting_date": meeting_date,
            "speaker": speaker,
            "content": content[:5000],
            "source_url": source_url,
            "fetched_at": datetime.utcnow().isoformat(),
        })

    for line in lines:
        if not line:
            continue
        speaker = match_speaker(line)
        if speaker:
            flush(current_speaker, buffer)
            current_speaker = speaker
            buffer = []
        else:
            buffer.append(line)

    flush(current_speaker, buffer)

    # 発言ブロードが全く検出されなかった場合: 段落ごとに分割
    if not records:
        paragraphs = re.split(r"\n{2,}", text)
        for para in paragraphs:
            para = para.strip()
            if len(para) < 20:
                continue
            records.append({
                "municipality_name": municipality_name,
                "meeting_name": meeting_name,
                "meeting_date": meeting_date,
                "speaker": "",
                "content": para[:5000],
                "source_url": source_url,
                "fetched_at": datetime.utcnow().isoformat(),
            })

    return records


def load_meta(pdf_path: Path) -> dict:
    """対応する .meta.json ファイルからメタ情報を読み込む。"""
    meta_path = pdf_path.with_suffix(".meta.json")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "municipality_name": pdf_path.parent.name,
        "source_url": str(pdf_path),
        "fetched_at": datetime.utcnow().isoformat(),
    }


def save_records(name: str, new_records: list[dict]):
    """既存の minutes.json に追記（重複 source_url を除外）。"""
    out_dir = PARSED_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "minutes.json"

    existing = []
    if out_file.exists():
        try:
            existing = json.loads(out_file.read_text(encoding="utf-8"))
        except Exception:
            existing = []

    existing_urls = {r.get("source_url") for r in existing}
    added = [r for r in new_records if r.get("source_url") not in existing_urls]
    all_records = existing + added

    out_file.write_text(
        json.dumps(all_records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return len(added)


def process_pdf(pdf_path: Path) -> int:
    """1つのPDFを処理してレコード数を返す。"""
    meta = load_meta(pdf_path)
    name = meta.get("municipality_name", pdf_path.parent.name)
    source_url = meta.get("source_url", str(pdf_path))

    try:
        text = extract_text_from_pdf(pdf_path)
    except Exception as e:
        print(f"  テキスト抽出エラー: {e}")
        return 0

    if not text.strip():
        print(f"  テキストなし（画像PDFの可能性）: {pdf_path.name}")
        return 0

    # メタ情報をテキストから補完
    meeting_name = meta.get("meeting_name") or extract_meeting_name(text[:1000])
    meeting_date = meta.get("meeting_date") or extract_date(text[:1000])

    records = split_into_speeches(text, name, meeting_name, meeting_date, source_url)
    added = save_records(name, records)
    return added


def main():
    pdf_files = sorted(RAW_DIR.glob("**/*.pdf"))

    if not pdf_files:
        print(f"PDFファイルが見つかりません: {RAW_DIR}")
        print("先に 02_scraper.py を実行してください。")
        sys.exit(0)

    print(f"PDF処理対象: {len(pdf_files)} 件")
    print("=" * 60)

    total_added = 0
    errors = 0

    for i, pdf_path in enumerate(pdf_files, start=1):
        name = pdf_path.parent.name
        print(f"[{i:02d}/{len(pdf_files):02d}] {name} / {pdf_path.name}")
        try:
            added = process_pdf(pdf_path)
            print(f"  -> {added} レコード追加")
            total_added += added
        except Exception as e:
            print(f"  -> エラー（スキップ）: {e}")
            errors += 1

    print("\n" + "=" * 60)
    print(f"完了: {total_added} レコード追加 / エラー {errors} 件")
    print(f"次のステップ: python 04_indexer.py  # DB に格納")


if __name__ == "__main__":
    main()
