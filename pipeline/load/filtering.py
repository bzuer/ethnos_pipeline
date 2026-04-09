"""Content filtering for the load pipeline.

Canonical EXCLUDED_WORK_TYPES (authoritative for the entire pipeline),
title/subtitle pattern matching, and the classify_ingest_exclusion function.
"""

import re
from typing import List, Optional, Tuple

from pipeline.load.normalize import _normalize_text_for_filter


# Canonical set — authoritative for the entire pipeline.
# Extract imports this set to filter early; load enforces at insertion.
EXCLUDED_WORK_TYPES = {
    "review",
    "peer-review",
    "book-review",
    "editorial",
    "erratum",
    "errata",
    "correction",
    "corrigendum",
    "retraction",
    "addendum",
    "letter",
    "note",
    "publisher-note",
    "paratext",
    "obituary",
    "acknowledgments",
    "journal-issue",
}

EXCLUDED_TITLE_EXACT = {
    "-",
    "--",
    "---",
    "n/a",
    "na",
    "none",
    "unknown",
    "untitled",
    "no title",
    "title unavailable",
    "abstract",
    "resumo",
    "resumen",
    "toc",
    "table of contents",
    "contents",
    "author index",
    "subject index",
    "books received",
    "publications received",
    "new books",
    "contributors",
    "list of contributors",
    "frontmatter",
    "front cover",
    "covers",
    "title page",
    "masthead",
    "copyright",
    "erratum",
    "abstracts",
    "summaries",
    "introduction",
    "international meetings",
    "annual meeting",
    "preface",
    "foreword",
    "avant-propos",
    "avant propos",
    "apresentacao",
    "presentacion",
}

EXCLUDED_TITLE_REGEX: List[Tuple[str, re.Pattern]] = [
    ("review-title", re.compile(r"\b(book\s+reviews?|resenhas?|resenas?|rezensionen?|comptes?-?rendus?)\b")),
    ("editorial-title", re.compile(r"\b(editorial|editorial note|editor s note|editors introduction|publisher s note|note from the editor|from the editor|letter from the editor)\b")),
    ("index-title", re.compile(r"\b(author index|subject index|table of contents|index to volume|volume index|contents of volume|volume contents|author and subject indexes?|subject and author indexes?|sumario|indice)\b|\btoc\b")),
    ("front-matter-title", re.compile(r"\b(front matter|back matter|cover and front matter|cover and back matter|issue information|frontmatter|front cover|title page|titelei|inhaltsverzeichnis|ficha tecnica|nota sobre a capa|paginas previas)\b")),
    ("acknowledgment-title", re.compile(r"\b(acknowledg(e)?ment(s)?|agradecimento(s)?|agradecimientos)\b")),
    ("contributor-note-title", re.compile(r"\b(contributors?|list of contributors|among the contributors|notes? on contributor(s)?|about the authors?|author biographies?|biographical notes?)\b")),
    ("errata-title", re.compile(r"\b(correction(s)?|corrigendum|corregendum|erratum|errata|retraction|expression of concern)\b")),
    ("announcement-title", re.compile(r"\b(announcement(s)?|call for applications|instructions for authors|future issues|forthcoming issues|international meetings?|annual meeting|conference announcement|conference program)\b")),
    ("conference-note-title", re.compile(r"^(the )?[ivxlcdm]+(st|nd|rd|th)? international conference on\b|\bmedical sociology group annual conference\b")),
    ("books-received-title", re.compile(r"\b(books?\s+received|publications?\s+received|new\s+books|publications?\s+recues|libros\s+recibidos|obras\s+recebidas)\b")),
    ("reviewer-note-title", re.compile(r"\b(thanks to reviewers|reviewers? for this)\b")),
]

# Subtitle patterns indicating a book review citation rather than a genuine subtitle.
REVIEW_SUBTITLE_REGEX: List[Tuple[str, re.Pattern]] = [
    ("review-byline-publisher", re.compile(
        r"(?:university.*press|press|publishers?|verlag)"
        r".*(?:\.? by [a-z]|\.? edited by [a-z]|\. ed\. by)",
        re.IGNORECASE,
    )),
    ("review-publisher-pages", re.compile(
        r"(?:university.*press|press|publishers?|verlag)"
        r".*\d+\s*pp",
        re.IGNORECASE,
    )),
    ("review-price-publisher", re.compile(
        r"[\$£€]\s*\d+\.\d{2}"
        r".*(?:press|publishers?|pp\.|isbn)",
        re.IGNORECASE,
    )),
]


def classify_ingest_exclusion(
    title: Optional[str],
    work_type: Optional[str],
    source: str = "unknown",
    subtitle: Optional[str] = None,
) -> Optional[str]:
    """Return exclusion reason string or None if the record should be loaded."""
    normalized_type = _normalize_text_for_filter(work_type)
    if normalized_type in EXCLUDED_WORK_TYPES:
        return f"{source}:work_type={normalized_type}"

    normalized_title = _normalize_text_for_filter(title)
    if not normalized_title:
        return f"{source}:blank_title"

    if normalized_title in EXCLUDED_TITLE_EXACT:
        return f"{source}:title_exact={normalized_title}"

    for label, pattern in EXCLUDED_TITLE_REGEX:
        if pattern.search(normalized_title):
            return f"{source}:title_pattern={label}"

    if subtitle and isinstance(subtitle, str) and len(subtitle) > 40:
        for label, pattern in REVIEW_SUBTITLE_REGEX:
            if pattern.search(subtitle):
                return f"{source}:subtitle_pattern={label}"

    return None
