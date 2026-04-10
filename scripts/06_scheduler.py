#!/usr/bin/env python3
"""
06_scheduler.py - 議事録定期更新パイプライン

02_scraper → 03_pdf_parser → 04_indexer を順次実行し、
結果を Slack Webhook で通知する。

cron での使い方:
  0 6 * * 1 cd /path/to/lobbyai-local && python scripts/06_scheduler.py

GitHub Actions での使い方:
  .github/workflows/update.yml から呼び出す（READMEを参照）

環境変数:
  SLACK_WEBHOOK_URL  Slack Incoming Webhook URL（未設定時は通知スキップ）
  DRY_RUN            1 を設定すると実際のスクレイプをスキップ（通知テスト用）
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import urllib.request
import urllib.error

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "db" / "minutes.db"
SCRIPTS_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"

PIPELINE = [
    ("スクレイプ",      "02_scraper.py"),
    ("PDF解析",        "03_pdf_parser.py"),
    ("DBインデックス",  "04_indexer.py"),
]


# ──────────────────────────────────────────────
# Slack 通知
# ──────────────────────────────────────────────

def post_slack(webhook_url: str, payload: dict) -> bool:
    """Slack Webhook に JSON を POST する。成功時 True。"""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.URLError as e:
        print(f"[Slack] 送信エラー: {e}", file=sys.stderr)
        return False


def notify_start(webhook_url: str, run_id: str):
    if not webhook_url:
        return
    post_slack(webhook_url, {
        "text": f":hourglass: *議事録更新開始* `{run_id}`\n開始時刻: {_now_jst()}",
    })


def notify_success(webhook_url: str, run_id: str, stats: dict):
    if not webhook_url:
        return
    lines = [
        f":white_check_mark: *議事録更新完了* `{run_id}`",
        f"完了時刻: {_now_jst()}",
        f"経過時間: {stats['elapsed_sec']:.0f}秒",
        f"新規レコード: *{stats['new_records']:,}* 件",
        f"総レコード: {stats['total_records']:,} 件",
    ]
    if stats.get("step_results"):
        lines.append("各ステップ:")
        for name, ok, secs in stats["step_results"]:
            icon = ":white_check_mark:" if ok else ":x:"
            lines.append(f"  {icon} {name} ({secs:.0f}秒)")
    post_slack(webhook_url, {"text": "\n".join(lines)})


def notify_error(webhook_url: str, run_id: str, step: str, error: str):
    if not webhook_url:
        return
    post_slack(webhook_url, {
        "text": (
            f":x: *議事録更新エラー* `{run_id}`\n"
            f"ステップ: {step}\n"
            f"エラー時刻: {_now_jst()}\n"
            f"```{error[:800]}```"
        ),
    })


def _now_jst() -> str:
    jst_offset = 9 * 3600
    now = datetime.now(timezone.utc)
    jst = datetime.fromtimestamp(now.timestamp() + jst_offset)
    return jst.strftime("%Y-%m-%d %H:%M JST")


# ──────────────────────────────────────────────
# DB レコード数取得
# ──────────────────────────────────────────────

def get_record_count() -> int:
    if not DB_PATH.exists():
        return 0
    try:
        conn = sqlite3.connect(str(DB_PATH))
        count = conn.execute("SELECT COUNT(*) FROM minutes").fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


# ──────────────────────────────────────────────
# ステップ実行
# ──────────────────────────────────────────────

def run_step(script_name: str, dry_run: bool, log_path: Path) -> tuple[bool, str, float]:
    """
    1つのスクリプトを subprocess で実行する。
    返り値: (成功, 出力テキスト, 経過秒)
    """
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        return False, f"{script_path} が見つかりません", 0.0

    cmd = [sys.executable, str(script_path)]
    env = os.environ.copy()

    if dry_run:
        print(f"  [DRY RUN] スキップ: {script_name}")
        return True, "[dry-run]", 0.0

    print(f"  実行中: {script_name} ...", end="", flush=True)
    t0 = time.monotonic()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR),
            env=env,
            timeout=3600,  # 最大1時間
        )
        elapsed = time.monotonic() - t0
        output = result.stdout + result.stderr

        # ログ保存
        log_path.write_text(output, encoding="utf-8")

        if result.returncode == 0:
            print(f" 完了 ({elapsed:.0f}秒)")
            return True, output, elapsed
        else:
            print(f" 失敗 (exit={result.returncode}, {elapsed:.0f}秒)")
            return False, output, elapsed

    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        msg = f"{script_name} がタイムアウトしました ({elapsed:.0f}秒)"
        print(f" タイムアウト")
        return False, msg, elapsed
    except Exception as e:
        elapsed = time.monotonic() - t0
        return False, traceback.format_exc(), elapsed


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="議事録定期更新パイプライン")
    parser.add_argument("--dry-run", action="store_true",
                        help="スクレイプをスキップして通知のみテスト")
    parser.add_argument("--no-slack", action="store_true",
                        help="Slack 通知を強制オフ")
    args = parser.parse_args()

    dry_run = args.dry_run or os.environ.get("DRY_RUN") == "1"
    webhook_url = "" if args.no_slack else os.environ.get("SLACK_WEBHOOK_URL", "")

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print(f"=== 議事録更新パイプライン [{run_id}] ===")
    print(f"  dry_run: {dry_run}")
    print(f"  Slack:   {'有効' if webhook_url else '無効（SLACK_WEBHOOK_URL 未設定）'}")
    print("=" * 50)

    count_before = get_record_count()
    notify_start(webhook_url, run_id)
    pipeline_start = time.monotonic()

    step_results = []
    pipeline_ok = True

    for step_name, script_name in PIPELINE:
        print(f"\n[{step_name}]")
        log_path = LOG_DIR / f"{run_id}_{script_name}.log"
        ok, output, elapsed = run_step(script_name, dry_run, log_path)
        step_results.append((step_name, ok, elapsed))

        if not ok:
            pipeline_ok = False
            error_summary = output[-1000:] if len(output) > 1000 else output
            notify_error(webhook_url, run_id, step_name, error_summary)
            print(f"  エラー詳細: {log_path}")
            # エラーでも次ステップへ続行（indexer は実行できるデータで動く）

    elapsed_total = time.monotonic() - pipeline_start
    count_after = get_record_count()
    new_records = max(0, count_after - count_before)

    stats = {
        "elapsed_sec": elapsed_total,
        "new_records": new_records,
        "total_records": count_after,
        "step_results": step_results,
    }

    print("\n" + "=" * 50)
    print(f"パイプライン完了: {elapsed_total:.0f}秒")
    print(f"新規レコード: {new_records:,} 件 (合計 {count_after:,} 件)")

    if pipeline_ok or new_records > 0:
        notify_success(webhook_url, run_id, stats)

    sys.exit(0 if pipeline_ok else 1)


if __name__ == "__main__":
    main()
