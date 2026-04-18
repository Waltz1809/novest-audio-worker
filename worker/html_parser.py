"""
html_parser.py — HTML to Plain Text Converter
===============================================
Strip HTML tags và chuẩn hóa văn bản tiếng Việt trước khi đưa vào TTS.
"""

import logging
import re
import unicodedata

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Ký tự / cụm cần loại bỏ hoặc thay thế trước khi TTS
_REPLACEMENTS = [
    # Thay thế dấu câu đặc biệt
    (r'[""„"]', '"'),
    (r"[''‚']", "'"),
    (r"[–—]", ", "),
    (r"…", "..."),
    (r"&amp;", "&"),
    (r"&lt;", "<"),
    (r"&gt;", ">"),
    (r"&nbsp;", " "),
    (r"&quot;", '"'),
    # Loại bỏ ký tự đặc biệt không phổ biến
    (r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", ""),
    # Chuẩn hóa khoảng trắng thừa
    (r"[ \t]+", " "),
    # Tối đa 2 dòng trống liên tiếp
    (r"\n{3,}", "\n\n"),
]


class HTMLParser:
    """Chuyển đổi HTML sang văn bản thuần túy tối ưu cho TTS."""

    @staticmethod
    def to_plain_text(html: str) -> str:
        """
        Chuyển HTML → plain text cho TTS.

        1. Parse bằng BeautifulSoup (lxml)
        2. Bảo toàn ngắt đoạn (p, br, div)
        3. Strip HTML tags
        4. Normalize Unicode (NFC)
        5. Áp dụng replacements
        6. Tách thành câu vừa phải
        """
        if not html:
            return ""

        # Parse HTML
        soup = BeautifulSoup(html, "lxml")

        # Thêm newline sau các block elements để bảo toàn cấu trúc đoạn văn
        for tag in soup.find_all(["p", "div", "br", "h1", "h2", "h3", "h4", "li"]):
            if tag.name == "br":
                tag.replace_with("\n")
            else:
                tag.append("\n")

        # Lấy text thuần
        text = soup.get_text(separator=" ")

        # Normalize Unicode
        text = unicodedata.normalize("NFC", text)

        # Áp dụng replacements
        for pattern, replacement in _REPLACEMENTS:
            text = re.sub(pattern, replacement, text)

        # Strip từng dòng và loại bỏ dòng trống
        lines = [line.strip() for line in text.splitlines()]
        cleaned = "\n".join(line for line in lines if line)

        if not cleaned:
            log.warning("Kết quả sau parsing rỗng")
        else:
            log.debug(f"Parsed: {len(cleaned)} chars từ {len(html)} chars HTML")

        return cleaned

    @staticmethod
    def split_into_chunks(text: str, max_words: int = 500) -> list[str]:
        """
        Chia văn bản thành các đoạn nhỏ (~max_words từ) để batch TTS.
        Cố gắng chia tại ranh giới câu (dấu chấm, chấm hỏi, chấm than).
        """
        if not text:
            return []

        # Chia theo câu (. ! ?)
        sentences = re.split(r"(?<=[.!?])\s+", text)
        chunks: list[str] = []
        current: list[str] = []
        current_words = 0

        for sentence in sentences:
            words = sentence.split()
            if current_words + len(words) > max_words and current:
                chunks.append(" ".join(current))
                current = []
                current_words = 0
            current.extend(words)
            current_words += len(words)

        if current:
            chunks.append(" ".join(current))

        log.debug(f"Chia thành {len(chunks)} chunks (max {max_words} từ/chunk)")
        return chunks
