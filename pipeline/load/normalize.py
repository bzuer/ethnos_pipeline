"""Data normalization functions for the load pipeline.

DOI, identifier, language, term, person name, text cleanup, and timestamp
normalization used across all load scripts.
"""

import re
import html
import unicodedata
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pipeline.common import normalize_issn  # noqa: F401 — canonical, re-exported


# ---------------------------------------------------------------------------
# DOI normalization
# ---------------------------------------------------------------------------

DOI_PREFIX_REGEX = re.compile(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", re.IGNORECASE)
DOI_SHAPE_REGEX = re.compile(r"^10\.\S+/\S+$", re.IGNORECASE)


def normalize_doi(raw_doi: Optional[str]) -> Optional[str]:
    if raw_doi is None:
        return None
    if not isinstance(raw_doi, str):
        raw_doi = str(raw_doi)

    doi = html.unescape(raw_doi).strip()
    if not doi:
        return None

    doi = DOI_PREFIX_REGEX.sub("", doi)
    doi = doi.replace("\u00A0", " ")
    doi = re.sub(r"\s+", "", doi)
    doi = doi.strip().strip("\"'")
    doi = re.sub(r"[.,;:]+$", "", doi)
    doi = doi.lower()

    if not doi or not DOI_SHAPE_REGEX.match(doi):
        return None
    return doi


# ---------------------------------------------------------------------------
# External identifier normalization
# ---------------------------------------------------------------------------

def normalize_openalex_id(value: Optional[str]) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    if "openalex.org/" in value:
        value = value.split("/")[-1]
    value = value.strip()
    return value[:50] if value else None


def normalize_ror_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        if "ror.org/" in value:
            value = value.split("ror.org/")[-1]
        if "/" in value:
            value = value.split("/")[-1]
        value = value.strip()
        return value[:20] if value else None
    return None


def normalize_wikidata_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        if "wikidata.org/wiki/" in value:
            value = value.split("wikidata.org/wiki/")[-1]
        if match := re.search(r"(Q\d+)", value):
            return match.group(1)[:20]
        value = value.strip()
        return value[:20] if value else None
    return None


def normalize_mag_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = str(int(value))
    if isinstance(value, str):
        if "/" in value:
            value = value.split("/")[-1]
        value = value.strip()
        return value[:50] if value else None
    return None


def normalize_language_code(value: Optional[str]) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    lang = value.strip()
    if not lang:
        return None
    if "-" in lang:
        lang = lang.split("-")[0]
    if "_" in lang:
        lang = lang.split("_")[0]
    lang = lang.lower()
    return lang[:3] if lang else None


def normalize_term_key(term: str) -> str:
    if not term:
        return ""
    cleaned = re.sub(r"[-_/,:;.\'\"`()\[\]&|]+", " ", term.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------

def clean_ingest_text(value: Optional[str]) -> Optional[str]:
    """HTML unescape, strip tags, collapse whitespace."""
    if not value or not isinstance(value, str):
        return None
    cleaned = html.unescape(value)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _normalize_text_for_filter(value: Optional[str]) -> str:
    """NFKD normalize, strip combining chars, remove non-alphanumeric."""
    if not value or not isinstance(value, str):
        return ""
    normalized = unicodedata.normalize("NFKD", value.strip().lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def format_iso_timestamp(timestamp_str: Optional[str]) -> Optional[str]:
    """Convert ISO 8601 timestamp to MySQL YYYY-MM-DD HH:MM:SS format."""
    if not timestamp_str:
        return None
    try:
        if "." in timestamp_str:
            timestamp_str = timestamp_str.split(".")[0]
        dt_object = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).replace(tzinfo=None)
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        logging.warning(f"Invalid or unexpected timestamp format: '{timestamp_str}'")
        return None
    except Exception as e:
        logging.error(f"Error formatting timestamp '{timestamp_str}': {e}")
        return None


# ---------------------------------------------------------------------------
# Person name normalization
# ---------------------------------------------------------------------------

def _normalize_person_name_for_matching(name: str) -> Optional[str]:
    if not name:
        return None
    cleaned_name = name.lower()
    cleaned_name = cleaned_name.replace("\u200E", "").replace("\u200F", "").replace("\u200B", "").replace("\u00A0", " ")
    cleaned_name = re.sub(r"[.,\"`()]+", "", cleaned_name)
    cleaned_name = re.sub(r"\s+", " ", cleaned_name).strip()
    if len(cleaned_name) < 2:
        return None
    return cleaned_name


def _strip_name_punctuation(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = re.sub(r"[.,;]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _prepare_person_name(author_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    surname = author_data.get("family")
    given_names = author_data.get("given")
    display_name = author_data.get("display_name") or author_data.get("name")

    def _split_comma_name(raw_name: str):
        parts = [p.strip() for p in raw_name.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[0], " ".join(parts[1:])
        if parts:
            return parts[0], parts[0]
        return None

    if (surname and "," in surname) or (given_names and "," in given_names):
        combined = " ".join(filter(None, [given_names, surname]))
        split_result = _split_comma_name(combined)
        if split_result:
            surname, given_names = split_result

    if not surname and not given_names and display_name:
        name = display_name.strip()
        split_result = _split_comma_name(name) if "," in name else None
        if split_result:
            surname, given_names = split_result
        else:
            parts = name.split(" ")
            if len(parts) > 1:
                given_names, surname = " ".join(parts[:-1]), parts[-1]
            elif parts:
                surname = given_names = parts[0]

    if not surname and given_names:
        surname = given_names
    if not given_names and surname:
        given_names = surname

    clean_given = _strip_name_punctuation(given_names)
    clean_surname = _strip_name_punctuation(surname)
    preferred_name = " ".join([p for p in (clean_given, clean_surname) if p]).strip()[:255]
    return {
        "given_names": clean_given[:255] if clean_given else None,
        "surname": clean_surname[:100] if clean_surname else None,
        "preferred_name": preferred_name or None,
    }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def extract_first_text(value: Optional[object]) -> Optional[str]:
    """Return first string from a list or string directly."""
    if isinstance(value, list):
        if not value:
            return None
        first = value[0]
        return first if isinstance(first, str) else None
    if isinstance(value, str):
        return value
    return None
