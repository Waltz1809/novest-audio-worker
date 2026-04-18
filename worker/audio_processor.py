"""
audio_processor.py — WAV → HLS Converter
==========================================
Convert file WAV đầu ra từ TTS sang HLS (HTTP Live Streaming):
    WAV → Opus → HLS (.m3u8 + .ts segments)

Yêu cầu: ffmpeg phải được cài đặt và có trong PATH.
"""

import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

# Cấu hình HLS
HLS_SEGMENT_DURATION = 10       # giây mỗi segment
HLS_AUDIO_BITRATE = "64k"       # Opus bitrate (64kbps là đủ cho audiobook)
HLS_AUDIO_CODEC = "libopus"     # Opus codec (tốt hơn AAC cho giọng nói)
HLS_FORMAT = "hls"


def _run_ffmpeg(cmd: list[str]) -> subprocess.CompletedProcess:
    """Chạy lệnh ffmpeg và raise nếu lỗi."""
    log.debug(f"ffmpeg: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"ffmpeg stderr:\n{result.stderr}")
        raise RuntimeError(f"ffmpeg thất bại (code {result.returncode})")
    return result


class AudioProcessor:
    """
    Xử lý audio: WAV → Opus → HLS.
    Output: thư mục chứa playlist.m3u8 và các file seg*.ts
    """

    def convert_to_hls(self, wav_path: Path, output_dir: Path) -> dict:
        """
        Convert 1 file WAV thành HLS segments.

        Args:
            wav_path: Đường dẫn file WAV đầu vào
            output_dir: Thư mục chứa output HLS

        Returns:
            {
                "playlist_path": Path,
                "segment_count": int,
                "duration_seconds": float,
            }
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        playlist_path = output_dir / "playlist.m3u8"

        # ffmpeg: WAV → Opus codec → HLS segments
        # -hls_list_size 0: giữ tất cả segments trong playlist (không xóa cũ)
        # -hls_time: độ dài mỗi segment (giây)
        # -hls_segment_filename: pattern tên file segment
        cmd = [
            "ffmpeg", "-y",
            "-i", str(wav_path),
            "-c:a", HLS_AUDIO_CODEC,
            "-b:a", HLS_AUDIO_BITRATE,
            "-vn",                          # không có video
            "-f", HLS_FORMAT,
            "-hls_time", str(HLS_SEGMENT_DURATION),
            "-hls_list_size", "0",
            "-hls_segment_filename", str(output_dir / "seg%03d.ts"),
            str(playlist_path),
        ]

        log.info(f"Converting {wav_path.name} → HLS ({HLS_AUDIO_CODEC} {HLS_AUDIO_BITRATE})")
        _run_ffmpeg(cmd)

        # Đếm segments và tổng duration
        segments = sorted(output_dir.glob("seg*.ts"))
        segment_count = len(segments)
        duration_seconds = self._get_duration(wav_path)

        log.info(
            f"HLS output: {segment_count} segments, "
            f"{duration_seconds:.1f}s → {playlist_path}"
        )

        return {
            "playlist_path": playlist_path,
            "segment_count": segment_count,
            "duration_seconds": duration_seconds,
        }

    @staticmethod
    def _get_duration(audio_path: Path) -> float:
        """Lấy thời lượng file audio bằng ffprobe."""
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            str(audio_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            import json
            try:
                info = json.loads(result.stdout)
                return float(info["format"]["duration"])
            except (KeyError, ValueError, json.JSONDecodeError):
                pass
        return 0.0
