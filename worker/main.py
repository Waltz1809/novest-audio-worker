#!/usr/bin/env python3
"""
Novest Audio Worker — Entry Point
==================================
Worker Python chạy local trên máy GPU, tự động lấy chapter từ Novest API,
gen audio bằng VieNeu-TTS, convert sang HLS (Opus), upload R2, báo hoàn thành.

Modes:
    batch  --novel-id X --chapters A-B   : Xử lý một đoạn chapter cụ thể
    watch  --interval N                  : Quét tự động mỗi N giây

Usage:
    python main.py --mode batch --novel-id 5 --chapters 1-50
    python main.py --mode watch --interval 300
    python main.py --mode batch --limit 3
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from api_client import NovEstAPIClient
from audio_processor import AudioProcessor
from html_parser import HTMLParser
from r2_uploader import R2Uploader
from tts_engine import VieNeuTTSEngine

# ─── Cấu hình logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("worker.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Load .env ──────────────────────────────────────────────────────
load_dotenv()


def process_chapter(task: dict, api: NovEstAPIClient, tts: VieNeuTTSEngine,
                     processor: AudioProcessor, uploader: R2Uploader) -> bool:
    """
    Pipeline xử lý 1 chapter:
    1. HTML → plain text
    2. Text → WAV (VieNeu TTS)
    3. WAV → Opus → HLS (.m3u8 + .ts segments)
    4. Upload từng segment lên R2 via presigned URL
    5. Báo hoàn thành qua API
    """
    chapter_id = task["chapterId"]
    chapter_title = task["chapterTitle"]
    novel_slug = task["novelSlug"]

    log.info(f"━━━ Bắt đầu xử lý: [{chapter_id}] {novel_slug} — {chapter_title} ━━━")

    tmp_dir = Path(os.getenv("TMP_DIR", "/tmp/audio")) / str(chapter_id)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    wav_path = tmp_dir / "output.wav"
    hls_dir = tmp_dir / "hls"
    hls_dir.mkdir(exist_ok=True)

    try:
        # Bước 1: HTML → plain text
        log.info(f"[{chapter_id}] Bước 1/5 — Parsing HTML...")
        plain_text = HTMLParser.to_plain_text(task["htmlContent"])
        if not plain_text.strip():
            raise ValueError("Nội dung chương rỗng sau khi strip HTML")
        log.info(f"[{chapter_id}] → {len(plain_text)} ký tự")

        # Bước 2: Text → WAV
        log.info(f"[{chapter_id}] Bước 2/5 — Tổng hợp giọng nói TTS...")
        tts.synthesize(plain_text, wav_path)
        log.info(f"[{chapter_id}] → Saved: {wav_path}")

        # Bước 3: WAV → HLS
        log.info(f"[{chapter_id}] Bước 3/5 — Convert WAV → HLS...")
        result = processor.convert_to_hls(wav_path, hls_dir)
        log.info(
            f"[{chapter_id}] → {result['segment_count']} segments, "
            f"{result['duration_seconds']:.1f}s"
        )

        # Bước 4: Upload R2
        log.info(f"[{chapter_id}] Bước 4/5 — Upload lên R2...")
        playlist_key = uploader.upload_hls(chapter_id, hls_dir, api)
        log.info(f"[{chapter_id}] → Playlist key: {playlist_key}")

        # Bước 5: Báo hoàn thành
        log.info(f"[{chapter_id}] Bước 5/5 — Báo hoàn thành...")
        api.complete_chapter(
            chapter_id=chapter_id,
            hls_playlist_key=playlist_key,
            duration_seconds=int(result["duration_seconds"]),
            segment_count=result["segment_count"],
        )
        log.info(f"[{chapter_id}] ✓ XONG!")
        return True

    except Exception as e:
        log.error(f"[{chapter_id}] ✗ LỖI: {e}", exc_info=True)
        try:
            api.fail_chapter(chapter_id=chapter_id, error_msg=str(e))
        except Exception as report_err:
            log.error(f"[{chapter_id}] Không thể báo lỗi về server: {report_err}")
        return False

    finally:
        # Dọn tmp files
        import shutil
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
            log.debug(f"[{chapter_id}] Cleaned up tmp: {tmp_dir}")


def run_batch(api: NovEstAPIClient, tts: VieNeuTTSEngine,
              processor: AudioProcessor, uploader: R2Uploader,
              novel_id: int | None, chapters: str | None, limit: int) -> None:
    """Mode batch: xử lý một lần rồi thoát."""
    log.info(f"Mode: BATCH | novel_id={novel_id} | chapters={chapters} | limit={limit}")

    params = {"limit": limit}
    if novel_id:
        params["novelId"] = novel_id

    tasks = api.get_tasks(**params)
    if not tasks:
        log.info("Không có chapter nào cần xử lý.")
        return

    log.info(f"Nhận được {len(tasks)} task(s)")

    # Filter theo chapter range nếu có
    if chapters and novel_id:
        try:
            parts = chapters.split("-")
            ch_from = int(parts[0])
            ch_to = int(parts[1]) if len(parts) > 1 else int(parts[0])
            tasks = [t for t in tasks if ch_from <= t["chapterId"] <= ch_to]
            log.info(f"Sau filter range [{ch_from}-{ch_to}]: {len(tasks)} task(s)")
        except ValueError:
            log.warning(f"Không thể parse --chapters '{chapters}', bỏ qua filter")

    ok = 0
    for task in tasks:
        if process_chapter(task, api, tts, processor, uploader):
            ok += 1

    log.info(f"━━━ Tổng kết: {ok}/{len(tasks)} thành công ━━━")


def run_watch(api: NovEstAPIClient, tts: VieNeuTTSEngine,
              processor: AudioProcessor, uploader: R2Uploader,
              interval: int, limit: int) -> None:
    """Mode watch: quét vô hạn mỗi `interval` giây."""
    log.info(f"Mode: WATCH | interval={interval}s | limit_per_batch={limit}")
    log.info("Gõ Ctrl+C để dừng.\n")

    cycle = 0
    while True:
        cycle += 1
        log.info(f"─── Chu kỳ #{cycle} ───")
        try:
            tasks = api.get_tasks(limit=limit)
            if not tasks:
                log.info("Không có task mới, chờ...")
            else:
                log.info(f"Nhận {len(tasks)} task(s)")
                for task in tasks:
                    process_chapter(task, api, tts, processor, uploader)
        except Exception as e:
            log.error(f"Lỗi trong chu kỳ #{cycle}: {e}", exc_info=True)

        log.info(f"Nghỉ {interval}s đến chu kỳ tiếp theo...\n")
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Novest Audio Worker — TTS pipeline cho chapter audiobook",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "watch"],
        default="batch",
        help="batch: chạy 1 lần | watch: quét vô hạn (default: batch)",
    )
    parser.add_argument(
        "--novel-id",
        type=int,
        default=None,
        help="ID của novel cần xử lý (chỉ dùng với --mode batch)",
    )
    parser.add_argument(
        "--chapters",
        type=str,
        default=None,
        help="Khoảng chapter ID cần xử lý: '1-50' (chỉ dùng với --mode batch + --novel-id)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Khoảng cách giữa các chu kỳ scan tính bằng giây (default: 300)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Số chapter tối đa mỗi batch (default: 5)",
    )

    args = parser.parse_args()

    # ─── Kiểm tra env ───────────────────────────────────────────────
    required_env = ["NOVEST_API_URL", "WORKER_TOTP_SECRET", "TTS_API_URL"]
    missing = [k for k in required_env if not os.getenv(k)]
    if missing:
        log.error(f"Thiếu biến môi trường: {', '.join(missing)}")
        log.error("Sao chép .env.example thành .env và điền đầy đủ.")
        sys.exit(1)

    # ─── Khởi tạo clients ────────────────────────────────────────────
    machine_name = os.getenv("WORKER_MACHINE", "unknown-machine")
    api = NovEstAPIClient(
        base_url=os.getenv("NOVEST_API_URL", "https://novest.me"),
        totp_secret=os.getenv("WORKER_TOTP_SECRET", ""),
        machine_name=machine_name,
    )
    tts = VieNeuTTSEngine(api_url=os.getenv("TTS_API_URL", "http://localhost:23333/v1"))
    processor = AudioProcessor()
    uploader = R2Uploader()

    log.info(f"Novest Audio Worker khởi động — Machine: {machine_name}")
    log.info(f"API: {api.base_url} | TTS: {tts.api_url}")

    # ─── Chạy theo mode ──────────────────────────────────────────────
    if args.mode == "batch":
        run_batch(api, tts, processor, uploader, args.novel_id, args.chapters, args.limit)
    else:
        run_watch(api, tts, processor, uploader, args.interval, args.limit)


if __name__ == "__main__":
    main()
