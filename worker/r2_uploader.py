"""
r2_uploader.py — Cloudflare R2 HLS Uploader
=============================================
Upload từng HLS segment và playlist file lên R2 thông qua presigned URLs
được cấp bởi Novest API.

Luồng:
    1. Với mỗi file .ts: POST /api/worker/presign → lấy uploadUrl
    2. PUT uploadUrl với nội dung file
    3. Sau khi tất cả segments xong: Upload playlist.m3u8
    4. Return playlist R2 key để báo về server
"""

import logging
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# MIME types cho HLS
MIME_PLAYLIST = "application/vnd.apple.mpegurl"
MIME_SEGMENT = "video/mp2t"

# Số luồng upload song song tối đa
MAX_UPLOAD_WORKERS = 4


class R2Uploader:
    """Upload HLS output lên Cloudflare R2 qua presigned URLs từ Novest API."""

    def upload_hls(self, chapter_id: int, hls_dir: Path, api) -> str:
        """
        Upload tất cả segments và playlist lên R2.

        Args:
            chapter_id: ID của chapter
            hls_dir: Thư mục chứa playlist.m3u8 và seg*.ts
            api: NovEstAPIClient instance

        Returns:
            R2 key của playlist.m3u8 (để lưu vào DB)
        """
        playlist_path = hls_dir / "playlist.m3u8"
        segment_paths = sorted(hls_dir.glob("seg*.ts"))

        if not playlist_path.exists():
            raise FileNotFoundError(f"Không tìm thấy playlist: {playlist_path}")

        if not segment_paths:
            raise FileNotFoundError(f"Không có segment nào trong: {hls_dir}")

        log.info(f"[{chapter_id}] Upload {len(segment_paths)} segments + playlist...")

        # Upload segments song song
        playlist_key: str = ""
        segment_keys: list[str] = []

        def upload_segment(seg_path: Path) -> str:
            presign = api.get_presign_url(
                chapter_id=chapter_id,
                file_name=seg_path.name,
                content_type=MIME_SEGMENT,
            )
            self._put_file(presign["uploadUrl"], seg_path, MIME_SEGMENT)
            return presign["key"]

        with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:
            futures = {executor.submit(upload_segment, seg): seg for seg in segment_paths}
            for i, future in enumerate(as_completed(futures)):
                seg = futures[future]
                try:
                    key = future.result()
                    segment_keys.append(key)
                    log.debug(f"[{chapter_id}] ✓ {seg.name} ({i + 1}/{len(segment_paths)})")
                except Exception as e:
                    log.error(f"[{chapter_id}] ✗ Upload lỗi {seg.name}: {e}")
                    raise

        log.info(f"[{chapter_id}] Tất cả {len(segment_keys)} segments đã upload")

        # Upload playlist.m3u8 cuối cùng
        # (đảm bảo tất cả segments đã sẵn sàng trước khi playlist xuất hiện)
        presign = api.get_presign_url(
            chapter_id=chapter_id,
            file_name="playlist.m3u8",
            content_type=MIME_PLAYLIST,
        )
        self._put_file(presign["uploadUrl"], playlist_path, MIME_PLAYLIST)
        playlist_key = presign["key"]

        log.info(f"[{chapter_id}] ✓ Playlist uploaded: {playlist_key}")
        return playlist_key

    @staticmethod
    def _put_file(upload_url: str, file_path: Path, content_type: str) -> None:
        """
        Upload 1 file lên presigned URL bằng HTTP PUT.
        """
        with file_path.open("rb") as f:
            resp = requests.put(
                upload_url,
                data=f,
                headers={"Content-Type": content_type},
                timeout=120,  # 2 phút tối đa per file
            )
        if not resp.ok:
            raise RuntimeError(
                f"Upload thất bại ({resp.status_code}): {file_path.name}\n{resp.text[:200]}"
            )
