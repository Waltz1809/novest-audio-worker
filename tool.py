#!/usr/bin/env python3
"""
Novest Audio Manual Tool
=========================
Workflow:
  Bước 1 — Tải nội dung chương:
    python tool.py fetch --novel-id 5
    python tool.py fetch --novel-id 5 --limit 20
    python tool.py fetch --novel-id 5 --chapters 1-50

  Bước 2 — Mở VieNeu web UI (http://127.0.0.1:7860), paste nội dung từ
            chapters/<id>_<slug>/content.txt, gen audio, Save WAV vào đúng folder đó.

  Bước 3 — Upload lên R2:
    python tool.py upload chapters/123_ten-chuong/
    python tool.py upload-all                      (upload tất cả folder có WAV)

  Nếu fetch rồi bỏ dở, reset để fetch lại:
    python tool.py reset chapters/123_ten-chuong/
"""

import argparse
import json
import os
import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from typing import Optional

import pyotp
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ─── Config ────────────────────────────────────────────────────────────────────
API_URL     = os.getenv("NOVEST_API_URL", "https://novest.me").rstrip("/")
TOTP_SECRET = os.getenv("WORKER_TOTP_SECRET", "")
MACHINE     = os.getenv("WORKER_MACHINE", "manual-tool")
CHAPTERS_DIR = Path("chapters")


# ─── Auth ──────────────────────────────────────────────────────────────────────
def _otp_headers() -> dict[str, str]:
    otp = pyotp.TOTP(TOTP_SECRET).now()
    return {
        "X-Worker-OTP": otp,
        "Content-Type": "application/json",
        "User-Agent": f"NovEstTool/{MACHINE}",
    }


# ─── HTML → plain text ─────────────────────────────────────────────────────────
def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["p", "div", "h1", "h2", "h3", "h4", "li"]):
        tag.append("\n")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r'[ \t]+', " ", text)
    text = re.sub(r'\n{3,}', "\n\n", text)
    lines = [ln.strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


# ─── ffmpeg helpers ────────────────────────────────────────────────────────────
def _wav_to_hls(wav_path: Path, hls_dir: Path) -> list[Path]:
    hls_dir.mkdir(parents=True, exist_ok=True)
    playlist = hls_dir / "playlist.m3u8"
    cmd = [
        "ffmpeg", "-y",
        "-i", str(wav_path),
        "-c:a", "libopus", "-b:a", "64k", "-vn",
        "-f", "hls",
        "-hls_time", "10",
        "-hls_list_size", "0",
        "-hls_segment_filename", str(hls_dir / "seg%03d.ts"),
        str(playlist),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ffmpeg lỗi:\n{result.stderr[-800:]}")
        sys.exit(1)
    return sorted(hls_dir.glob("seg*.ts"))


def _get_duration(audio_path: Path) -> float:
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(audio_path)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        try:
            return float(json.loads(result.stdout)["format"]["duration"])
        except Exception:
            pass
    return 0.0


# ─── R2 upload helper ──────────────────────────────────────────────────────────
def _upload_file(chapter_id: int, file_path: Path, content_type: str) -> str:
    """Upload 1 file lên R2 qua presigned URL. Trả về R2 key."""
    resp = requests.post(
        f"{API_URL}/api/worker/presign",
        headers=_otp_headers(),
        json={"chapterId": chapter_id, "fileName": file_path.name, "contentType": content_type},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    with file_path.open("rb") as f:
        put = requests.put(data["uploadUrl"], data=f, headers={"Content-Type": content_type}, timeout=120)
    if not put.ok:
        raise RuntimeError(f"PUT thất bại ({put.status_code}): {file_path.name}\n{put.text[:200]}")

    return data["key"]


# ─── COMMAND: fetch ────────────────────────────────────────────────────────────
def cmd_fetch(novel_id: Optional[int], chapters_range: Optional[str], limit: int) -> None:
    params: dict = {"limit": limit}
    if novel_id:
        params["novelId"] = novel_id

    print(f"Đang lấy danh sách chương từ {API_URL}...")
    resp = requests.get(f"{API_URL}/api/worker/tasks", headers=_otp_headers(), params=params, timeout=30)
    resp.raise_for_status()
    tasks = resp.json()

    if not tasks:
        print("Không có chương nào cần gen audio (hoặc tất cả đã có audio).")
        print("Kiểm tra: novel có bật membershipEnabled và approvalStatus = APPROVED chưa?")
        return

    # Filter theo range nếu có (--chapters 1-50 → filter theo chapterId, không phải số thứ tự)
    if chapters_range and novel_id:
        try:
            parts = chapters_range.split("-")
            ch_from = int(parts[0])
            ch_to   = int(parts[1]) if len(parts) > 1 else int(parts[0])
            tasks = [t for t in tasks if ch_from <= t["chapterId"] <= ch_to]
        except ValueError:
            print(f"Warning: Không parse được --chapters '{chapters_range}', bỏ qua filter.")

    print(f"Nhận được {len(tasks)} chương:\n")
    CHAPTERS_DIR.mkdir(exist_ok=True)

    for task in tasks:
        cid        = task["chapterId"]
        slug       = task["chapterSlug"]
        title      = task["chapterTitle"]
        novel_slug = task["novelSlug"]
        novel_id_  = task["novelId"]

        safe_slug = re.sub(r"[^\w\-]", "_", slug)[:60]
        folder = CHAPTERS_DIR / f"{cid}_{safe_slug}"
        folder.mkdir(exist_ok=True)

        # meta.json — dùng để upload sau
        meta = {
            "chapterId":    cid,
            "chapterSlug":  slug,
            "chapterTitle": title,
            "novelId":      novel_id_,
            "novelSlug":    novel_slug,
        }
        (folder / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # content.txt — paste vào VieNeu web UI
        text = html_to_text(task.get("htmlContent", ""))
        (folder / "content.txt").write_text(text, encoding="utf-8")

        word_count = len(text.split())
        print(f"  ✓ [{cid}] {title}")
        print(f"     Folder : {folder}")
        print(f"     Nội dung: {word_count} từ (~{word_count // 130 + 1} phút audio)\n")

    print("─" * 60)
    print("Bước tiếp theo:")
    print("  1. Mở VieNeu web UI: uv run vieneu-web  (http://127.0.0.1:7860)")
    print("  2. Paste nội dung từ content.txt của từng folder")
    print("  3. Gen audio → Save WAV vào đúng folder đó")
    print("  4. Chạy: python tool.py upload <folder>")
    print("     hoặc : python tool.py upload-all")


# ─── COMMAND: upload ───────────────────────────────────────────────────────────
def cmd_upload(folder_path: str) -> None:
    folder = Path(folder_path)
    meta_file = folder / "meta.json"

    if not meta_file.exists():
        print(f"Lỗi: Không tìm thấy meta.json trong {folder}")
        print("Hãy chạy 'fetch' trước.")
        return

    meta       = json.loads(meta_file.read_text(encoding="utf-8"))
    chapter_id = meta["chapterId"]
    title      = meta["chapterTitle"]

    # Tìm WAV
    wav_files = list(folder.glob("*.wav"))
    if not wav_files:
        print(f"[{chapter_id}] Chưa có file WAV trong {folder}/")
        print(f"  → Gen audio từ VieNeu web UI rồi save .wav vào folder đó.")
        return

    wav_path = wav_files[0]
    print(f"\n[{chapter_id}] {title}")
    print(f"  WAV: {wav_path.name} ({wav_path.stat().st_size / 1024 / 1024:.1f} MB)")

    # WAV → HLS
    hls_dir = folder / "hls"
    print(f"  Converting WAV → HLS...")
    segments = _wav_to_hls(wav_path, hls_dir)
    duration = _get_duration(wav_path)
    print(f"  → {len(segments)} segments, {duration:.0f}s")

    # Upload segments
    print(f"  Uploading {len(segments)} segments...")
    for i, seg in enumerate(segments):
        _upload_file(chapter_id, seg, "video/mp2t")
        print(f"    {seg.name} ({i + 1}/{len(segments)})", end="\r")
    print(f"    {len(segments)} segments ✓                ")

    # Upload playlist
    playlist_key = _upload_file(chapter_id, hls_dir / "playlist.m3u8", "application/vnd.apple.mpegurl")
    print(f"  playlist.m3u8 ✓  →  {playlist_key}")

    # Báo DONE
    resp = requests.post(
        f"{API_URL}/api/worker/complete",
        headers=_otp_headers(),
        json={
            "chapterId":      chapter_id,
            "hlsPlaylistKey": playlist_key,
            "durationSeconds": int(duration),
            "segmentCount":   len(segments),
            "workerMachine":  MACHINE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    print(f"  ✓ XONG — audio đã live trên web!\n")


# ─── COMMAND: upload-all ───────────────────────────────────────────────────────
def cmd_upload_all() -> None:
    if not CHAPTERS_DIR.exists():
        print("Chưa có thư mục chapters/. Hãy chạy fetch trước.")
        return

    folders = [f for f in sorted(CHAPTERS_DIR.iterdir())
               if f.is_dir() and list(f.glob("*.wav")) and (f / "meta.json").exists()]

    if not folders:
        print("Không có folder nào vừa có meta.json vừa có file WAV.")
        return

    print(f"Tìm thấy {len(folders)} folder cần upload:\n")
    for folder in folders:
        cmd_upload(str(folder))

    print(f"Hoàn tất {len(folders)} chương.")


# ─── COMMAND: reset ────────────────────────────────────────────────────────────
def cmd_reset(folder_path: str) -> None:
    """Báo FAILED để chapter được fetch lại lần sau."""
    folder = Path(folder_path)
    meta = json.loads((folder / "meta.json").read_text(encoding="utf-8"))
    chapter_id = meta["chapterId"]

    resp = requests.post(
        f"{API_URL}/api/worker/fail",
        headers=_otp_headers(),
        json={"chapterId": chapter_id, "errorMsg": "Reset thủ công bởi tool"},
        timeout=30,
    )
    resp.raise_for_status()
    print(f"[{chapter_id}] Reset về FAILED ✓ — sẽ xuất hiện lại ở lần fetch tiếp theo.")


# ─── COMMAND: list ─────────────────────────────────────────────────────────────
def cmd_list() -> None:
    """Liệt kê các folder và trạng thái."""
    if not CHAPTERS_DIR.exists():
        print("Chưa có thư mục chapters/.")
        return

    folders = sorted(CHAPTERS_DIR.iterdir())
    if not folders:
        print("chapters/ trống.")
        return

    print(f"{'Folder':<50} {'WAV':>5} {'HLS':>5}")
    print("─" * 65)
    for f in folders:
        if not f.is_dir():
            continue
        has_wav = "✓" if list(f.glob("*.wav")) else "✗"
        has_hls = "✓" if (f / "hls" / "playlist.m3u8").exists() else "✗"
        print(f"{f.name:<50} {has_wav:>5} {has_hls:>5}")


# ─── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    if not TOTP_SECRET:
        print("Lỗi: WORKER_TOTP_SECRET chưa được set trong .env")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Novest Audio Manual Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # fetch
    p_fetch = sub.add_parser("fetch", help="Tải nội dung chương về")
    p_fetch.add_argument("--novel-id", type=int, help="ID của novel")
    p_fetch.add_argument("--chapters", type=str, help="Range chapterId: '100-200'")
    p_fetch.add_argument("--limit", type=int, default=10, help="Số chương tối đa (default: 10)")

    # upload
    p_upload = sub.add_parser("upload", help="Upload WAV → HLS → R2 cho 1 folder")
    p_upload.add_argument("folder", help="chapters/<id>_<slug>/")

    # upload-all
    sub.add_parser("upload-all", help="Upload tất cả folder có WAV")

    # reset
    p_reset = sub.add_parser("reset", help="Reset chương về FAILED để fetch lại")
    p_reset.add_argument("folder", help="chapters/<id>_<slug>/")

    # list
    sub.add_parser("list", help="Xem trạng thái các folder")

    args = parser.parse_args()

    if args.cmd == "fetch":
        cmd_fetch(args.novel_id, args.chapters, args.limit)
    elif args.cmd == "upload":
        cmd_upload(args.folder)
    elif args.cmd == "upload-all":
        cmd_upload_all()
    elif args.cmd == "reset":
        cmd_reset(args.folder)
    elif args.cmd == "list":
        cmd_list()


if __name__ == "__main__":
    main()
