from __future__ import annotations

import os
import io
import re
import tempfile
import subprocess
import unicodedata
from typing import Optional

from openai_clients import get_transcription_client, TRANSCRIBE_MODEL
from log_config import get_logger

log = get_logger("pera.speech")


# Devanagari Unicode block: U+0900..U+097F. Any character in this range
# is Hindi script. If Whisper returned Devanagari, the speaker was almost
# certainly speaking Urdu (phonetically near-identical to Hindi) and we
# need to transliterate to Roman-Urdu so the downstream retriever and
# answerer can process it — the PERA corpus is in English + Roman-Urdu,
# not Devanagari.
_DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")


# Devanagari -> Roman-Urdu transliteration table.
# Inherent "a" handling: Hindi consonants carry an implicit short "a"
# unless followed by a vowel sign or the virama (halant). We implement
# that by adding "a" for each consonant and deleting it when a vowel
# sign or virama follows.
_DEV_VOWELS = {
    "\u0905": "a",   # अ
    "\u0906": "aa",  # आ
    "\u0907": "i",   # इ
    "\u0908": "ee",  # ई
    "\u0909": "u",   # उ
    "\u090A": "oo",  # ऊ
    "\u090B": "ri",  # ऋ
    "\u090F": "e",   # ए
    "\u0910": "ai",  # ऐ
    "\u0913": "o",   # ओ
    "\u0914": "au",  # औ
}

_DEV_VOWEL_SIGNS = {
    "\u093E": "aa",  # ा
    "\u093F": "i",   # ि
    "\u0940": "ee",  # ी
    "\u0941": "u",   # ु
    "\u0942": "oo",  # ू
    "\u0943": "ri",  # ृ
    "\u0945": "o",   # ॅ  candra-e (used in loan words like "record")
    "\u0946": "e",   # ॆ  short-e
    "\u0947": "e",   # े
    "\u0948": "ai",  # ै
    "\u0949": "o",   # ॉ  candra-o (loan words like "college")
    "\u094A": "o",   # ॊ  short-o
    "\u094B": "o",   # ो
    "\u094C": "au",  # ौ
}

_DEV_CONSONANTS = {
    "\u0915": "k",   "\u0916": "kh",  "\u0917": "g",   "\u0918": "gh",  "\u0919": "n",
    "\u091A": "ch",  "\u091B": "chh", "\u091C": "j",   "\u091D": "jh",  "\u091E": "ny",
    "\u091F": "t",   "\u0920": "th",  "\u0921": "d",   "\u0922": "dh",  "\u0923": "n",
    "\u0924": "t",   "\u0925": "th",  "\u0926": "d",   "\u0927": "dh",  "\u0928": "n",
    "\u092A": "p",   "\u092B": "ph",  "\u092C": "b",   "\u092D": "bh",  "\u092E": "m",
    "\u092F": "y",   "\u0930": "r",   "\u0932": "l",   "\u0935": "w",
    "\u0936": "sh",  "\u0937": "sh",  "\u0938": "s",   "\u0939": "h",
    "\u0931": "r",   "\u0933": "l",   "\u0934": "l",
    # Nukta-derived Urdu sounds often produced by Whisper on Urdu input
    "\u0958": "q",   "\u0959": "kh",  "\u095A": "gh",  "\u095B": "z",
    "\u095C": "r",   "\u095D": "rh",  "\u095E": "f",   "\u095F": "y",
}

# Virama (halant) — suppresses inherent "a"
_DEV_VIRAMA = "\u094D"

# Chandrabindu / anusvara / visarga
_DEV_MARKS = {
    "\u0902": "n",   # anusvara
    "\u0901": "n",   # candrabindu
    "\u0903": "h",   # visarga
    "\u093C": "",    # nukta (already handled on composed forms)
}

# Digits
_DEV_DIGITS = {
    "\u0966": "0", "\u0967": "1", "\u0968": "2", "\u0969": "3", "\u096A": "4",
    "\u096B": "5", "\u096C": "6", "\u096D": "7", "\u096E": "8", "\u096F": "9",
}


def _transliterate_devanagari(text: str) -> str:
    """Transliterate Devanagari text to Roman-Urdu-ish Latin letters.

    This is a pragmatic ITRANS-adjacent mapping. It produces text that
    is phonetically close to how the same Urdu sentence would be
    written in Roman-Urdu — good enough for the downstream RAG
    pipeline, which already handles Roman-Urdu queries (see
    retriever.py's Roman-Urdu rewriter).
    """
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        # Consonant handling — add "a" unless vowel sign / virama follows
        if ch in _DEV_CONSONANTS:
            out.append(_DEV_CONSONANTS[ch])
            nxt = text[i + 1] if i + 1 < n else ""
            if nxt == _DEV_VIRAMA:
                i += 2  # skip virama, no inherent vowel
                continue
            if nxt in _DEV_VOWEL_SIGNS:
                out.append(_DEV_VOWEL_SIGNS[nxt])
                i += 2
                continue
            # No virama, no vowel sign — emit inherent short "a"
            out.append("a")
            i += 1
            continue
        if ch in _DEV_VOWELS:
            out.append(_DEV_VOWELS[ch])
            i += 1
            continue
        if ch in _DEV_DIGITS:
            out.append(_DEV_DIGITS[ch])
            i += 1
            continue
        if ch in _DEV_MARKS:
            out.append(_DEV_MARKS[ch])
            i += 1
            continue
        if ch == _DEV_VIRAMA:
            i += 1  # orphan virama — skip
            continue
        # Pass-through for anything non-Devanagari (spaces, punctuation,
        # Latin letters that Whisper already produced).
        out.append(ch)
        i += 1
    result = "".join(out)
    # Normalize excess whitespace.
    result = re.sub(r"\s+", " ", result).strip()
    return result


def _looks_like_devanagari(text: str) -> bool:
    """Return True if the text contains Devanagari characters."""
    return bool(_DEVANAGARI_RE.search(text or ""))


def _guess_ext(audio_bytes: bytes) -> str:
    """
    Best-effort magic header detection.
    """
    if not audio_bytes:
        return ".bin"

    head = audio_bytes[:16]

    # WAV: "RIFF....WAVE"
    if head[:4] == b"RIFF" and b"WAVE" in head:
        return ".wav"

    # MP3: "ID3" or MPEG frame sync 0xFF 0xFB
    if head[:3] == b"ID3" or (len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xE0) == 0xE0):
        return ".mp3"

    # OGG: "OggS"
    if head[:4] == b"OggS":
        return ".ogg"

    # WEBM/Matroska: 1A 45 DF A3
    if head[:4] == b"\x1a\x45\xdf\xa3":
        return ".webm"

    # M4A/MP4 often: "....ftyp"
    if len(head) >= 12 and head[4:8] == b"ftyp":
        return ".m4a"

    return ".bin"


def _ffmpeg_exists() -> bool:
    try:
        p = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True)
        return p.returncode == 0
    except Exception:
        return False


def _convert_to_wav(in_path: str) -> Optional[str]:
    """
    Convert input audio to WAV using ffmpeg (if installed).
    Returns wav path or None if conversion fails.
    """
    if not _ffmpeg_exists():
        return None

    out_path = in_path + ".wav"
    cmd = [
        "ffmpeg",
        "-y",
        "-i", in_path,
        "-ac", "1",
        "-ar", "16000",
        out_path
    ]

    try:
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            return None
        if not os.path.exists(out_path) or os.path.getsize(out_path) < 1024:
            return None
        return out_path
    except Exception:
        return None


def transcribe_audio(audio_bytes: bytes, model: str = None) -> str:
    """
    Safe transcription:
    - Never raises to caller
    - Returns readable error text if unsupported/corrupt
    """
    if not audio_bytes or len(audio_bytes) < 800:
        log.warning("Transcription rejected: audio too short (%d bytes)", len(audio_bytes) if audio_bytes else 0)
        return "⚠️ I could not read the audio (empty/too short). Please record again."

    model = model or TRANSCRIBE_MODEL

    ext = _guess_ext(audio_bytes)
    log.info("Transcription starting: ext=%s, size=%d bytes, model=%s", ext, len(audio_bytes), model)

    try:
        with tempfile.TemporaryDirectory() as td:
            raw_path = os.path.join(td, f"audio{ext}")
            with open(raw_path, "wb") as f:
                f.write(audio_bytes)

            # If file is unknown/webm, try converting to wav for maximum compatibility
            use_path = raw_path
            if ext in (".webm", ".bin", ".m4a", ".ogg"):
                wav = _convert_to_wav(raw_path)
                if wav:
                    use_path = wav
                    log.debug("Converted %s to WAV for Whisper", ext)

            # Call OpenAI transcription.
            #
            # IMPORTANT: Whisper auto-detects language from audio. When
            # the speaker uses Urdu or Roman-Urdu, Whisper frequently
            # identifies the language as Hindi and emits Devanagari
            # script — because Hindi and Urdu are phonetically almost
            # identical and Hindi is the higher-frequency training
            # label. The PERA chatbot's retriever and answerer operate
            # on English + Roman-Urdu (Latin script) only, so
            # Devanagari output breaks the pipeline.
            #
            # Strategy:
            #   1. Pass a Roman-Urdu prompt so Whisper prefers that
            #      transcription style when the input is ambiguous.
            #   2. If the returned text still contains Devanagari
            #      characters, transliterate them to Roman-Urdu
            #      Latin letters via _transliterate_devanagari.
            client = get_transcription_client()
            # Vocabulary hint only — PERA-specific terms help the model
            # recognise acronyms and proper nouns. No language bias in the
            # prompt itself: the `language` parameter below controls script.
            #
            # CRITICAL: pass `language="en"` to force Latin-script output.
            # Without it, Pakistani/Indian-accented English is often
            # misclassified as Hindi and returned in Devanagari, which our
            # transliterator then mangles into phonetic gibberish like
            # "panjaapa inphorsmenta" for spoken "Punjab enforcement".
            #
            # With language="en":
            #   - Pure English speech -> clean English output
            #   - Urdu/Roman-Urdu speech -> Whisper transcribes phonetically
            #     in Latin letters (natural Roman-Urdu), which is what the
            #     downstream RAG pipeline expects.
            prompt = (
                "PERA, Punjab Enforcement and Regulatory Authority, "
                "EPO, Enforcement Protection Order, hearing officer, "
                "Schedule 3, SSO, KPI, compliance, notification."
            )
            with open(use_path, "rb") as f:
                result = client.audio.transcriptions.create(
                    model=model,
                    file=f,
                    prompt=prompt,
                    language="en",
                    response_format="text",
                )

            # response_format="text" returns a plain string; other formats
            # return an object with a .text attribute. Handle both.
            if isinstance(result, str):
                text = result.strip()
            else:
                text = (getattr(result, "text", None) or "").strip()
            if not text:
                log.warning("Transcription returned empty text")
                return "⚠️ I could not transcribe the audio. Please try again with clearer speech."

            if _looks_like_devanagari(text):
                original = text
                text = _transliterate_devanagari(text)
                log.info(
                    "Devanagari detected in Whisper output — transliterated "
                    "to Roman-Urdu. before=%r after=%r",
                    original[:80], text[:80],
                )

            log.info("Transcription complete: %d chars", len(text))
            return text

    except Exception as e:
        # Never crash the app
        msg = str(e)
        log.error("Transcription failed: %s", msg, exc_info=True)
        if "corrupted" in msg.lower() or "unsupported" in msg.lower() or "invalid_value" in msg.lower():
            return "⚠️ Audio format not supported. Please record again. (If this continues, install FFmpeg for conversion.)"
        return f"⚠️ Voice transcription failed: {msg}"
