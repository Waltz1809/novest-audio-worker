"""
tool.py — Novest Audio Core Library
=====================================
Import bởi app.py (Gradio UI) và dùng trực tiếp qua CLI.

CLI usage:
  python tool.py fetch --novel-id 5 --limit 20
  python tool.py upload chapters/123_ten-chuong/
  python tool.py upload-all
  python tool.py list
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
API_URL      = os.getenv("NOVEST_API_URL", "https://novest.me").rstrip("/")
TOTP_SECRET  = os.getenv("WORKER_TOTP_SECRET", "")
MACHINE      = os.getenv("WORKER_MACHINE", "manual-tool")
CHAPTERS_DIR = Path("chapters")

# ─── Bộ lọc ký tự đặc biệt ────────────────────────────────────────────────────
_REPLACEMENTS = [
    (r'[""„"]',              '"'),   # curly double quotes → thẳng
    (r"[''‚']",              "'"),   # curly single quotes → thẳng
    (r"[–—]",                ", "),  # dash → dấu phẩy
    (r"…",                   "..."), # ellipsis
    (r"&amp;",               "&"),
    (r"&lt;",                "<"),
    (r"&gt;",                ">"),
    (r"&nbsp;",              " "),
    (r"&quot;",              '"'),
    (r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", ""),  # control chars
    (r"[ \t]+",              " "),   # khoảng trắng thừa
    (r"\n{3,}",              "\n\n"), # tối đa 2 dòng trống
]


# ─── Auth ──────────────────────────────────────────────────────────────────────
def _otp_headers() -> dict[str, str]:
    return {
        "X-Worker-OTP": pyotp.TOTP(TOTP_SECRET).now(),
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
    for pattern, replacement in _REPLACEMENTS:
        text = re.sub(pattern, replacement, text)
    lines = [ln.strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


# ─── ffmpeg helpers ────────────────────────────────────────────────────────────
HLS_BITRATE  = "48k"   # 32k=~4MB, 48k=~5.5MB, 64k=~8MB per 45min chapter
HLS_SEGMENT  = "10"   # giây mỗi segment


def wav_to_hls(wav_path: Path, hls_dir: Path) -> list[Path]:
    hls_dir.mkdir(parents=True, exist_ok=True)
    playlist  = hls_dir / "playlist.m3u8"
    init_path = hls_dir / "init.mp4"   # absolute path để ffmpeg tạo đúng chỗ

    cmd = [
        "ffmpeg", "-y",
        "-i", str(wav_path),
        "-c:a", "libopus", "-b:a", HLS_BITRATE, "-vn",
        "-f", "hls",
        "-hls_time", HLS_SEGMENT,
        "-hls_list_size", "0",
        "-hls_segment_type", "fmp4",
        "-hls_fmp4_init_filename", str(init_path),   # absolute path
        "-hls_segment_filename", str(hls_dir / "seg%03d.m4s"),
        str(playlist),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError(
            "Không tìm thấy ffmpeg. Cài ffmpeg và thêm vào PATH:\n"
            "  Windows: winget install ffmpeg\n"
            "  Hoặc tải tại: https://www.gyan.dev/ffmpeg/builds/"
        )
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg lỗi:\n{result.stderr[-600:]}")

    if not init_path.exists():
        files = [f.name for f in hls_dir.iterdir()]
        raise RuntimeError(f"init.mp4 không được tạo. Files trong hls/: {files}")

    # ffmpeg dùng absolute path trong playlist vì ta truyền absolute vào
    # → rewrite về relative "init.mp4" để presign route hoạt động đúng
    content = playlist.read_text(encoding="utf-8")
    content = content.replace(str(init_path).replace("\\", "/"), "init.mp4")
    content = content.replace(str(init_path), "init.mp4")
    playlist.write_text(content, encoding="utf-8")

    return sorted(hls_dir.glob("seg*.m4s"))


def get_duration(audio_path: Path) -> float:
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(audio_path)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        try:
            return float(json.loads(result.stdout)["format"]["duration"])
        except Exception:
            pass
    return 0.0


# ─── R2 upload ─────────────────────────────────────────────────────────────────
def upload_file_to_r2(chapter_id: int, file_path: Path, content_type: str) -> str:
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
        raise RuntimeError(f"PUT thất bại ({put.status_code}): {file_path.name}")
    return data["key"]


# ─── Core: fetch ───────────────────────────────────────────────────────────────
def fetch(
    novel_id: Optional[int] = None,
    chapters_range: Optional[str] = None,
    limit: int = 10,
) -> list[dict]:
    """
    Lấy nội dung chương từ server, lưu vào chapters/.
    Dùng /content (không side effect) thay vì /tasks.
    Trả về list[dict] với thông tin từng chương đã lưu.
    """
    if not novel_id:
        raise ValueError("Bắt buộc phải có novelId khi fetch")

    params: dict = {"limit": limit, "novelId": novel_id}
    resp = requests.get(f"{API_URL}/api/worker/content", headers=_otp_headers(), params=params, timeout=30)
    resp.raise_for_status()
    tasks: list[dict] = resp.json()

    if not tasks:
        return []

    # Filter theo chapters range (theo chapterId)
    if chapters_range and novel_id:
        try:
            parts = chapters_range.split("-")
            ch_from = int(parts[0])
            ch_to = int(parts[1]) if len(parts) > 1 else int(parts[0])
            tasks = [t for t in tasks if ch_from <= t["chapterId"] <= ch_to]
        except ValueError:
            pass

    CHAPTERS_DIR.mkdir(exist_ok=True)
    results = []

    for task in tasks:
        cid   = task["chapterId"]
        slug  = task["chapterSlug"]
        title = task["chapterTitle"]

        safe_slug = re.sub(r"[^\w\-]", "_", slug)[:60]
        folder = CHAPTERS_DIR / f"{cid}_{safe_slug}"
        folder.mkdir(exist_ok=True)

        meta = {
            "chapterId":    cid,
            "chapterSlug":  slug,
            "chapterTitle": title,
            "novelId":      task["novelId"],
            "novelSlug":    task["novelSlug"],
        }
        (folder / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        text = html_to_text(task.get("htmlContent", ""))
        content = f"{title}\n\n{text}"
        (folder / "content.txt").write_text(content, encoding="utf-8")

        word_count = len(text.split())
        results.append({
            "chapterId":    cid,
            "chapterTitle": title,
            "folder":       str(folder),
            "wordCount":    word_count,
            "estMinutes":   round(word_count / 130),
        })

    return results


# ─── Core: upload ──────────────────────────────────────────────────────────────
def upload(
    folder_path: str,
    on_progress: Optional[callable] = None,
) -> dict:
    """
    Convert WAV → HLS → upload R2 → báo DONE.
    on_progress(message: str) được gọi để cập nhật tiến trình.
    Trả về dict kết quả.
    """
    def log(msg: str):
        if on_progress:
            on_progress(msg)

    folder = Path(folder_path)
    meta = json.loads((folder / "meta.json").read_text(encoding="utf-8"))
    chapter_id = meta["chapterId"]
    title      = meta["chapterTitle"]

    wav_files = list(folder.glob("*.wav"))
    if not wav_files:
        raise FileNotFoundError(f"Không tìm thấy file .wav trong {folder}/")

    wav_path = wav_files[0]
    log(f"WAV: {wav_path.name} ({wav_path.stat().st_size / 1024 / 1024:.1f} MB)")

    # Claim chapter → PROCESSING trước khi upload
    log("Claiming chapter...")
    claim_resp = requests.post(
        f"{API_URL}/api/worker/claim",
        headers=_otp_headers(),
        json={"chapterId": chapter_id},
        timeout=30,
    )
    claim_resp.raise_for_status()

    log("Converting WAV → HLS...")
    hls_dir  = folder / "hls"
    segments = wav_to_hls(wav_path, hls_dir)
    duration = get_duration(wav_path)
    log(f"→ {len(segments)} segments, {duration:.0f}s")

    # Upload init segment (fMP4)
    init_mp4 = hls_dir / "init.mp4"
    if init_mp4.exists():
        upload_file_to_r2(chapter_id, init_mp4, "video/mp4")
        log("  init.mp4 ✓")

    log(f"Uploading {len(segments)} segments...")
    for i, seg in enumerate(segments):
        upload_file_to_r2(chapter_id, seg, "video/iso.segment")
        log(f"  {seg.name} ({i + 1}/{len(segments)})")

    playlist_key = upload_file_to_r2(chapter_id, hls_dir / "playlist.m3u8", "application/vnd.apple.mpegurl")
    log(f"  playlist.m3u8 ✓")

    resp = requests.post(
        f"{API_URL}/api/worker/complete",
        headers=_otp_headers(),
        json={
            "chapterId":       chapter_id,
            "hlsPlaylistKey":  playlist_key,
            "durationSeconds": int(duration),
            "segmentCount":    len(segments),
            "workerMachine":   MACHINE,
        },
        timeout=30,
    )
    resp.raise_for_status()

    return {
        "chapterId":    chapter_id,
        "chapterTitle": title,
        "playlistKey":  playlist_key,
        "duration":     duration,
        "segments":     len(segments),
    }


# ─── Core: list chapters ───────────────────────────────────────────────────────
def list_chapters() -> list[dict]:
    """
    Trả về trạng thái tất cả folders trong chapters/.
    """
    if not CHAPTERS_DIR.exists():
        return []

    results = []
    for folder in sorted(CHAPTERS_DIR.iterdir()):
        if not folder.is_dir() or not (folder / "meta.json").exists():
            continue
        meta      = json.loads((folder / "meta.json").read_text(encoding="utf-8"))
        has_wav   = bool(list(folder.glob("*.wav")))
        has_hls   = (folder / "hls" / "playlist.m3u8").exists()
        wav_name  = next((f.name for f in folder.glob("*.wav")), "—")
        results.append({
            "chapterId":    meta["chapterId"],
            "chapterTitle": meta["chapterTitle"],
            "folder":       folder.name,
            "hasWav":       has_wav,
            "wavFile":      wav_name,
            "hasHls":       has_hls,
            "status":       "✓ Uploaded" if has_hls else ("🎵 Ready" if has_wav else "⏳ Chờ WAV"),
        })
    return results


# ─── Core: reset chapter ───────────────────────────────────────────────────────
def reset(folder_path: str) -> None:
    """Báo FAILED để chapter được fetch lại."""
    meta = json.loads((Path(folder_path) / "meta.json").read_text(encoding="utf-8"))
    resp = requests.post(
        f"{API_URL}/api/worker/fail",
        headers=_otp_headers(),
        json={"chapterId": meta["chapterId"], "errorMsg": "Reset thủ công bởi tool"},
        timeout=30,
    )
    resp.raise_for_status()


# ─── Core: reset novel ─────────────────────────────────────────────────────────
def reset_novel(novel_id: int) -> int:
    """Reset tất cả chapter PROCESSING của novel về FAILED. Trả về số chapter đã reset."""
    resp = requests.post(
        f"{API_URL}/api/worker/reset",
        headers=_otp_headers(),
        json={"novelId": novel_id},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("resetCount", 0)


# ─── CLI wrapper ───────────────────────────────────────────────────────────────
def _cli_fetch(args) -> None:
    results = fetch(args.novel_id, args.chapters, args.limit)
    if not results:
        print("Không có chương nào. Kiểm tra novel có bật membershipEnabled chưa?")
        return
    print(f"\nĐã tải {len(results)} chương:\n")
    for r in results:
        print(f"  [{r['chapterId']}] {r['chapterTitle']}")
        print(f"       {r['wordCount']} từ · ~{r['estMinutes']} phút audio · {r['folder']}\n")
    print("─" * 55)
    print("Tiếp theo: mở VieNeu web UI, gen audio, save WAV vào đúng folder,")
    print("sau đó chạy: python tool.py upload-all")


def _cli_upload(folder_path: str) -> None:
    meta = json.loads((Path(folder_path) / "meta.json").read_text(encoding="utf-8"))
    print(f"\n[{meta['chapterId']}] {meta['chapterTitle']}")
    result = upload(folder_path, on_progress=lambda m: print(f"  {m}"))
    print(f"  ✓ XONG — audio live trên web!\n")


def _cli_upload_all() -> None:
    chapters = [c for c in list_chapters() if c["hasWav"] and not c["hasHls"]]
    if not chapters:
        print("Không có folder nào có WAV chờ upload.")
        return
    print(f"Upload {len(chapters)} chương...\n")
    for ch in chapters:
        _cli_upload(str(CHAPTERS_DIR / ch["folder"]))


def main() -> None:
    if not TOTP_SECRET:
        print("Lỗi: WORKER_TOTP_SECRET chưa set trong .env")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Novest Audio Tool", epilog=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    pf = sub.add_parser("fetch", help="Tải nội dung chương về")
    pf.add_argument("--novel-id", type=int)
    pf.add_argument("--chapters", type=str, help="Range chapterId: '100-200'")
    pf.add_argument("--limit", type=int, default=10)

    pu = sub.add_parser("upload", help="Upload WAV → HLS → R2")
    pu.add_argument("folder")

    sub.add_parser("upload-all", help="Upload tất cả folder có WAV")

    pr = sub.add_parser("reset", help="Reset chương về FAILED")
    pr.add_argument("folder")

    sub.add_parser("list", help="Xem trạng thái")

    args = parser.parse_args()

    if args.cmd == "fetch":
        _cli_fetch(args)
    elif args.cmd == "upload":
        _cli_upload(args.folder)
    elif args.cmd == "upload-all":
        _cli_upload_all()
    elif args.cmd == "reset":
        reset(args.folder)
        meta = json.loads((Path(args.folder) / "meta.json").read_text(encoding="utf-8"))
        print(f"[{meta['chapterId']}] Reset về FAILED ✓")
    elif args.cmd == "list":
        chapters = list_chapters()
        if not chapters:
            print("Chưa có gì trong chapters/")
            return
        for c in chapters:
            wav_info = f"WAV: {c['wavFile']}" if c["hasWav"] else "Chờ WAV"
            print(f"  [{c['chapterId']}] {c['status']:15s} {c['chapterTitle'][:40]}  ({wav_info})")


if __name__ == "__main__":
    main()
