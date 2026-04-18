"""
tts_engine.py — VieNeu TTS Engine Wrapper
==========================================
Gọi VieNeu-TTS Docker HTTP API (OpenAI-compatible /v1/audio/speech)
để tổng hợp giọng nói từ văn bản tiếng Việt.

Docker: pnnbao/vieneu-tts:serve (expose port 23333, LMDeploy backend)
"""

import logging
import os
from pathlib import Path

import requests

from html_parser import HTMLParser

log = logging.getLogger(__name__)

# Giọng đọc mặc định — có thể override qua env
DEFAULT_VOICE = os.getenv("TTS_VOICE", "nova")
# Tốc độ đọc: 0.25–4.0 (1.0 = bình thường)
DEFAULT_SPEED = float(os.getenv("TTS_SPEED", "1.0"))


class VieNeuTTSEngine:
    """
    Wrapper cho VieNeu-TTS Docker API.
    Tự động chia văn bản dài thành chunks → gen từng phần → ghép WAV.
    """

    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")
        self.speech_endpoint = f"{self.api_url}/audio/speech"
        log.info(f"VieNeu TTS endpoint: {self.speech_endpoint}")

    def _synthesize_chunk(self, text: str, output_path: Path, voice: str, speed: float) -> None:
        """
        Gọi API tổng hợp giọng nói cho 1 đoạn text.
        API trả về raw WAV binary.
        """
        payload = {
            "model": "tts-1",
            "input": text,
            "voice": voice,
            "speed": speed,
            "response_format": "wav",
        }

        resp = requests.post(
            self.speech_endpoint,
            json=payload,
            timeout=300,  # 5 phút tối đa per chunk
        )
        resp.raise_for_status()

        output_path.write_bytes(resp.content)
        size_kb = len(resp.content) / 1024
        log.debug(f"TTS chunk: {len(text)} chars → {size_kb:.1f} KB WAV")

    def synthesize(
        self,
        text: str,
        output_wav: Path,
        voice: str = DEFAULT_VOICE,
        speed: float = DEFAULT_SPEED,
    ) -> None:
        """
        Tổng hợp toàn bộ văn bản thành 1 file WAV.

        Nếu text dài (>500 từ), chia thành chunks và ghép lại.
        Dùng `ffmpeg` để concat các WAV chunks.
        """
        chunks = HTMLParser.split_into_chunks(text, max_words=500)
        log.info(f"TTS: {len(chunks)} chunk(s), tổng {len(text)} chars")

        if len(chunks) == 0:
            raise ValueError("Text rỗng, không thể TTS")

        if len(chunks) == 1:
            # Đơn giản: 1 chunk → 1 file trực tiếp
            self._synthesize_chunk(chunks[0], output_wav, voice, speed)
        else:
            # Nhiều chunks → gen từng phần rồi concat
            chunk_paths: list[Path] = []
            tmp_dir = output_wav.parent / "chunks"
            tmp_dir.mkdir(exist_ok=True)

            for i, chunk in enumerate(chunks):
                chunk_path = tmp_dir / f"chunk_{i:03d}.wav"
                log.info(f"TTS chunk {i + 1}/{len(chunks)} ({len(chunk)} chars)...")
                self._synthesize_chunk(chunk, chunk_path, voice, speed)
                chunk_paths.append(chunk_path)

            # Concat tất cả chunks thành 1 WAV
            self._concat_wavs(chunk_paths, output_wav)

            # Cleanup chunks
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)

        log.info(f"TTS hoàn tất → {output_wav} ({output_wav.stat().st_size / 1024 / 1024:.1f} MB)")

    @staticmethod
    def _concat_wavs(wav_paths: list[Path], output: Path) -> None:
        """Nối nhiều file WAV thành 1 bằng ffmpeg."""
        import subprocess

        # Tạo file list cho ffmpeg concat demuxer
        list_file = output.parent / "concat_list.txt"
        with list_file.open("w", encoding="utf-8") as f:
            for p in wav_paths:
                f.write(f"file '{p.as_posix()}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", str(list_file),
            "-c", "copy",
            str(output),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        list_file.unlink(missing_ok=True)

        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg concat lỗi:\n{result.stderr}")

        log.debug(f"Concat {len(wav_paths)} WAV → {output}")
