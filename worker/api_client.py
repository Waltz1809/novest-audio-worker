"""
api_client.py — Novest API Client
===================================
Gọi Novest HTTP API với TOTP authentication.
Tự động gen OTP code trước mỗi request.
"""

import logging
from typing import Any, Optional

import pyotp
import requests

log = logging.getLogger(__name__)


class NovEstAPIClient:
    """HTTP client cho Novest Worker API với TOTP auth."""

    def __init__(self, base_url: str, totp_secret: str, machine_name: str = "unknown"):
        self.base_url = base_url.rstrip("/")
        self.totp_secret = totp_secret
        self.machine_name = machine_name
        self._totp = pyotp.TOTP(totp_secret)

    def _otp_headers(self) -> dict[str, str]:
        """Tạo header X-Worker-OTP với OTP hiện tại."""
        otp = self._totp.now()
        return {
            "X-Worker-OTP": otp,
            "Content-Type": "application/json",
            "User-Agent": f"NovEstWorker/{self.machine_name}",
        }

    def get_tasks(self, limit: int = 5, novel_id: Optional[int] = None) -> list[dict]:
        """
        GET /api/worker/tasks
        Lấy danh sách chapter cần gen audio.
        """
        params: dict[str, Any] = {"limit": limit}
        if novel_id is not None:
            params["novelId"] = novel_id

        url = f"{self.base_url}/api/worker/tasks"
        log.debug(f"GET {url} params={params}")

        resp = requests.get(url, headers=self._otp_headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_presign_url(self, chapter_id: int, file_name: str, content_type: str) -> dict:
        """
        POST /api/worker/presign
        Lấy presigned URL để upload 1 file lên R2.

        Returns: { "uploadUrl": str, "key": str }
        """
        url = f"{self.base_url}/api/worker/presign"
        payload = {
            "chapterId": chapter_id,
            "fileName": file_name,
            "contentType": content_type,
        }
        log.debug(f"POST {url} body={payload}")

        resp = requests.post(url, headers=self._otp_headers(), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def complete_chapter(
        self,
        chapter_id: int,
        hls_playlist_key: str,
        duration_seconds: int,
        segment_count: int,
    ) -> dict:
        """
        POST /api/worker/complete
        Báo hoàn thành xử lý 1 chapter.
        """
        url = f"{self.base_url}/api/worker/complete"
        payload = {
            "chapterId": chapter_id,
            "hlsPlaylistKey": hls_playlist_key,
            "durationSeconds": duration_seconds,
            "segmentCount": segment_count,
            "workerMachine": self.machine_name,
        }
        log.debug(f"POST {url} body={payload}")

        resp = requests.post(url, headers=self._otp_headers(), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def fail_chapter(self, chapter_id: int, error_msg: str) -> dict:
        """
        POST /api/worker/fail
        Báo lỗi xử lý 1 chapter.
        """
        url = f"{self.base_url}/api/worker/fail"
        payload = {
            "chapterId": chapter_id,
            "errorMsg": error_msg[:2000],  # giới hạn độ dài
        }
        log.debug(f"POST {url} body={payload}")

        resp = requests.post(url, headers=self._otp_headers(), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()
