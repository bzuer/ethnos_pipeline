
import os
import configparser
import mariadb
import logging
import re
import unicodedata
import html
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

DEFAULT_CACHE_KEYS = (
    "persons_by_orcid",
    "persons_by_norm_name",
    "venues_by_issn",
    "venues_by_eissn",
    "venues_by_name",
    "venues_by_openalex",
    "organizations_by_norm_name",
    "organizations_by_ror",
    "organizations_by_openalex",
    "organizations_by_wikidata",
    "organizations_by_mag",
    "subjects_by_term",
)

EXCLUDED_WORK_TYPES = {
    "review",
    "peer-review",
    "book-review",
    "editorial",
    "erratum",
    "correction",
    "retraction",
    "addendum",
    "letter",
    "paratext",
    "obituary",
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
    "rejoinder",
    "discussion",
    "comment",
    "comments",
    "reply",
    "introduction",
    "international meetings",
    "annual meeting",
}

EXCLUDED_TITLE_REGEX: List[Tuple[str, re.Pattern]] = [
    ("review-title", re.compile(r"\b(book\s+review|review(s)?|review\s+of|resenha(s)?|resena(s)?|rezension(en)?|comptes?-?rendus)\b")),
    ("editorial-title", re.compile(r"\b(editorial|editorial note|editor s note|editors introduction|publisher s note|note from the editor|from the editor|letter from the editor)\b")),
    ("index-title", re.compile(r"\b(author index|subject index|table of contents|index to volume|volume index|contents of volume|volume contents|author and subject indexes?|subject and author indexes?|sumario|indice)\b|\btoc\b")),
    ("front-matter-title", re.compile(r"\b(front matter|back matter|cover and front matter|cover and back matter|issue information|frontmatter|front cover|title page|titelei|inhaltsverzeichnis|ficha tecnica|nota sobre a capa|paginas previas)\b")),
    ("acknowledgment-title", re.compile(r"\b(acknowledg(e)?ment(s)?|agradecimento(s)?|agradecimientos)\b")),
    ("contributor-note-title", re.compile(r"\b(contributors?|list of contributors|among the contributors|notes? on contributor(s)?|about the authors?|author biographies?|biographical notes?)\b")),
    ("errata-title", re.compile(r"\b(correction(s)?|corrigendum|corregendum|erratum|errata|retraction|expression of concern)\b")),
    ("announcement-title", re.compile(r"\b(announcement(s)?|notice(s)?|statement(s)?|call for applications|instructions for authors|future issues|forthcoming issues|international meetings?|annual meeting|conference announcement|conference program)\b")),
    ("commentary-response-title", re.compile(r"\b(commentary on|comments? on|reply to commentary|response to (the )?commentary|a response to|a reply to)\b|(\ba reply\b$)|(^rejoinder$)")),
    ("conference-note-title", re.compile(r"^(the )?[ivxlcdm]+(st|nd|rd|th)? international conference on\b|\bmedical sociology group annual conference\b")),
    ("preface-title", re.compile(r"\b(preface|foreword|avant propos|apresentacao|presentacion)\b")),
    ("books-received-title", re.compile(r"\b(books?\s+received|publications?\s+received|new\s+books|publications?\s+recues|libros\s+recibidos|obras\s+recebidas)\b")),
    ("reviewer-note-title", re.compile(r"\b(thanks to reviewers|reviewers? for this)\b")),
]

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


def build_cache() -> Dict[str, Dict]:
    return {k: {} for k in DEFAULT_CACHE_KEYS}


def _resolve_config_paths(filename: str) -> List[str]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(script_dir))
    return [
        os.path.join(script_dir, filename),
        os.path.join(repo_root, filename),
    ]


def read_db_config(filename: str = "config.ini", section: str = "database", config_path: Optional[str] = None) -> Dict[str, Any]:
    parser = configparser.ConfigParser()
    candidates = [config_path] if config_path else _resolve_config_paths(filename)
    read_ok = False
    for path in candidates:
        if path and parser.read(path):
            read_ok = True
            break
    if not read_ok:
        raise FileNotFoundError("Arquivo de configuração não encontrado.")
    db_config = dict(parser.items(section))
    if "port" in db_config:
        db_config["port"] = int(db_config["port"])
    if "name" in db_config:
        db_config["database"] = db_config.pop("name")
    return db_config


def get_connection(config_path: Optional[str] = None) -> mariadb.Connection:
    conn = None
    cursor = None
    try:
        db_config = read_db_config(config_path=config_path)
        conn_params = {k: v for k, v in db_config.items() if k not in ["charset", "collation"]}
        conn = mariadb.connect(**conn_params, connect_timeout=20, read_timeout=60, write_timeout=60)
        conn.autocommit = False
        cursor = conn.cursor()
        cursor.execute("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_uca1400_ai_ci'")
        cursor.close()
        logging.info("Conexão com o banco de dados estabelecida (utf8mb4_uca1400_ai_ci).")
        return conn
    except mariadb.Error as e:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.error(f"Erro fatal ao conectar ao MariaDB: {e}")
        raise


def ensure_connection(conn: Optional[mariadb.Connection], config_path: Optional[str] = None) -> mariadb.Connection:
    if conn and conn.open:
        ping = getattr(conn, "ping", None)
        if callable(ping):
            try:
                ping()
                return conn
            except mariadb.Error:
                try:
                    conn.close()
                except mariadb.Error:
                    pass
        else:
            cursor = None
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return conn
            except mariadb.Error:
                try:
                    conn.close()
                except mariadb.Error:
                    pass
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except mariadb.Error:
                        pass
    return get_connection(config_path)


def format_iso_timestamp(timestamp_str: Optional[str]) -> Optional[str]:
    if not timestamp_str:
        return None
    try:
        if "." in timestamp_str:
            timestamp_str = timestamp_str.split(".")[0]
        dt_object = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).replace(tzinfo=None)
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        logging.warning(f"Timestamp inválido ou formato inesperado: '{timestamp_str}'")
        return None
    except Exception as e:
        logging.error(f"Erro ao formatar timestamp '{timestamp_str}': {e}")
        return None


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

    def _split_comma_name(raw_name: str) -> Optional[tuple]:
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


def normalize_term_key(term: str) -> str:
    if not term:
        return ""
    cleaned = re.sub(r"[-_/,:;.\'\"`()\[\]&|]+", " ", term.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def extract_first_text(value: Optional[object]) -> Optional[str]:
    if isinstance(value, list):
        if not value:
            return None
        first = value[0]
        return first if isinstance(first, str) else None
    if isinstance(value, str):
        return value
    return None


LICENSE_VERSION_RANK = {
    "submittedversion": 1,
    "acceptedversion": 2,
    "publishedversion": 3,
}


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


def safe_rollback(connection, label: str) -> bool:
    if not connection or not connection.open:
        return False
    try:
        connection.rollback()
        return True
    except Exception as e:
        logging.error(f"{label} rollback failed: {e}")
        try:
            connection.close()
        except Exception:
            pass
        return False


def clean_ingest_text(value: Optional[str]) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    cleaned = html.unescape(value)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _normalize_text_for_filter(value: Optional[str]) -> str:
    if not value or not isinstance(value, str):
        return ""
    normalized = unicodedata.normalize("NFKD", value.strip().lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def classify_ingest_exclusion(title: Optional[str], work_type: Optional[str], source: str = "unknown") -> Optional[str]:
    normalized_type = _normalize_text_for_filter(work_type)
    if normalized_type in EXCLUDED_WORK_TYPES:
        return f"{source}:work_type={normalized_type}"

    normalized_title = _normalize_text_for_filter(title)
    if not normalized_title:
        return None

    if normalized_title in EXCLUDED_TITLE_EXACT:
        return f"{source}:title_exact={normalized_title}"

    for label, pattern in EXCLUDED_TITLE_REGEX:
        if pattern.search(normalized_title):
            return f"{source}:title_pattern={label}"
    return None


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


def get_or_create_person(cursor: mariadb.Cursor, author_data: Dict, cache: Dict[str, Dict]) -> Optional[int]:
    person_name = _prepare_person_name(author_data)
    surname = person_name["surname"]
    given_names = person_name["given_names"]
    preferred_name = person_name["preferred_name"]

    if not preferred_name:
        return None
    orcid = None
    orcid_raw = author_data.get("orcid") or author_data.get("ORCID")
    if orcid_raw:
        match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", orcid_raw)
        if match:
            orcid = match.group(1).upper()

    match_key = _normalize_person_name_for_matching(preferred_name)
    if not match_key:
        return None

    if orcid and orcid in cache.get("persons_by_orcid", {}):
        return cache["persons_by_orcid"][orcid]
    if not orcid and match_key in cache.get("persons_by_norm_name", {}):
        return cache["persons_by_norm_name"][match_key]

    person_id = None
    existing_orcid = None

    if orcid:
        cursor.execute("SELECT id, orcid FROM persons WHERE orcid = ?", (orcid,))
        if result := cursor.fetchone():
            person_id, existing_orcid = result

    if not person_id:
        cursor.execute("SELECT id, orcid FROM persons WHERE normalized_name = ?", (match_key,))
        if result := cursor.fetchone():
            person_id, existing_orcid = result

    if person_id:
        if orcid and not existing_orcid:
            cursor.execute("SELECT id FROM persons WHERE orcid = ? AND id != ?", (orcid, person_id))
            if not cursor.fetchone():
                try:
                    cursor.execute("UPDATE persons SET orcid = ? WHERE id = ?", (orcid, person_id))
                except mariadb.IntegrityError:
                    pass
        if orcid:
            cache.setdefault("persons_by_orcid", {})[orcid] = person_id
            cache.setdefault("persons_by_norm_name", {})[match_key] = person_id
        return person_id

    try:
        safe_family = surname[:100] if surname else None
        safe_given = given_names[:255] if given_names else None
        cursor.execute(
            "INSERT INTO persons (preferred_name, family_name, given_names, orcid) VALUES (?, ?, ?, ?)",
            (preferred_name, safe_family, safe_given, orcid),
        )
        person_id = cursor.lastrowid
        if person_id:
            if orcid:
                cache.setdefault("persons_by_orcid", {})[orcid] = person_id
            cache.setdefault("persons_by_norm_name", {})[match_key] = person_id
        return person_id
    except mariadb.IntegrityError:
        if orcid:
            cursor.execute("SELECT id FROM persons WHERE orcid = ?", (orcid,))
        else:
            cursor.execute("SELECT id FROM persons WHERE normalized_name = ?", (match_key,))
        if result := cursor.fetchone():
            return result[0]
        return None


def get_or_create_organization(
    cursor: mariadb.Cursor,
    name: Optional[str],
    org_type: str,
    cache: Dict[str, Dict],
    ror_id: Optional[str] = None,
    openalex_id: Optional[str] = None,
    wikidata_id: Optional[str] = None,
    mag_id: Optional[str] = None,
    url: Optional[str] = None,
    country_code: Optional[str] = None,
) -> Optional[int]:
    clean_name = name.strip()[:512] if name else None
    norm_name_cache_key = clean_name.lower() if clean_name else None
    ror_id = normalize_ror_id(ror_id)
    openalex_id = normalize_openalex_id(openalex_id)
    wikidata_id = normalize_wikidata_id(wikidata_id)
    mag_id = normalize_mag_id(mag_id)
    url = url.strip()[:512] if url and isinstance(url, str) else None
    country_code = country_code.strip().upper()[:2] if country_code and isinstance(country_code, str) else None

    if ror_id and ror_id in cache.get("organizations_by_ror", {}):
        return cache["organizations_by_ror"][ror_id]
    if openalex_id and openalex_id in cache.get("organizations_by_openalex", {}):
        return cache["organizations_by_openalex"][openalex_id]
    if wikidata_id and wikidata_id in cache.get("organizations_by_wikidata", {}):
        return cache["organizations_by_wikidata"][wikidata_id]
    if mag_id and mag_id in cache.get("organizations_by_mag", {}):
        return cache["organizations_by_mag"][mag_id]
    if norm_name_cache_key:
        cache_key = (norm_name_cache_key, org_type)
        if cache_key in cache.get("organizations_by_norm_name", {}):
            return cache["organizations_by_norm_name"][cache_key]

    org_id = None
    lookup_fields = (
        ("ror_id", ror_id),
        ("openalex_id", openalex_id),
        ("wikidata_id", wikidata_id),
        ("mag_id", mag_id),
    )
    for field, value in lookup_fields:
        if not value:
            continue
        cursor.execute(f"SELECT id FROM organizations WHERE {field} = ?", (value,))
        if result := cursor.fetchone():
            org_id = result[0]
            break

    if not org_id and norm_name_cache_key:
        cursor.execute(
            "SELECT id FROM organizations WHERE standardized_name = ? AND type = ?",
            (norm_name_cache_key, org_type),
        )
        if result := cursor.fetchone():
            org_id = result[0]

    if org_id:
        update_fields = []
        update_params = []
        if ror_id:
            update_fields.append("ror_id = COALESCE(ror_id, ?)")
            update_params.append(ror_id)
        if openalex_id:
            update_fields.append("openalex_id = COALESCE(openalex_id, ?)")
            update_params.append(openalex_id)
        if wikidata_id:
            update_fields.append("wikidata_id = COALESCE(wikidata_id, ?)")
            update_params.append(wikidata_id)
        if mag_id:
            update_fields.append("mag_id = COALESCE(mag_id, ?)")
            update_params.append(mag_id)
        if url:
            update_fields.append("url = COALESCE(url, ?)")
            update_params.append(url)
        if country_code:
            update_fields.append("country_code = COALESCE(country_code, ?)")
            update_params.append(country_code)
        if update_fields:
            update_params.append(org_id)
            try:
                cursor.execute(
                    "UPDATE organizations SET " + ", ".join(update_fields) + " WHERE id = ?",
                    tuple(update_params),
                )
            except mariadb.IntegrityError:
                pass
    else:
        if not clean_name:
            return None
        try:
            cursor.execute(
                "INSERT INTO organizations (name, type, country_code, ror_id, openalex_id, wikidata_id, mag_id, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (clean_name, org_type, country_code, ror_id, openalex_id, wikidata_id, mag_id, url),
            )
            org_id = cursor.lastrowid
        except mariadb.IntegrityError:
            for field, value in lookup_fields:
                if not value:
                    continue
                cursor.execute(f"SELECT id FROM organizations WHERE {field} = ?", (value,))
                if result := cursor.fetchone():
                    org_id = result[0]
                    break
            if not org_id and norm_name_cache_key:
                cursor.execute(
                    "SELECT id FROM organizations WHERE standardized_name = ? AND type = ?",
                    (norm_name_cache_key, org_type),
                )
                if result := cursor.fetchone():
                    org_id = result[0]

    if org_id:
        if norm_name_cache_key:
            cache.setdefault("organizations_by_norm_name", {})[(norm_name_cache_key, org_type)] = org_id
        if ror_id:
            cache.setdefault("organizations_by_ror", {})[ror_id] = org_id
        if openalex_id:
            cache.setdefault("organizations_by_openalex", {})[openalex_id] = org_id
        if wikidata_id:
            cache.setdefault("organizations_by_wikidata", {})[wikidata_id] = org_id
        if mag_id:
            cache.setdefault("organizations_by_mag", {})[mag_id] = org_id
    return org_id


def get_or_create_venue(
    cursor: mariadb.Cursor,
    name: Optional[str],
    print_issn: Optional[str],
    electronic_issn: Optional[str],
    venue_type: str,
    cache: Dict[str, Dict],
    openalex_id: Optional[str] = None,
) -> Optional[int]:
    clean_name = name.strip()[:512] if name else None
    norm_print_issn = print_issn.replace("-", "")[:9] if print_issn else None
    norm_electronic_issn = electronic_issn.replace("-", "")[:9] if electronic_issn else None
    openalex_id = normalize_openalex_id(openalex_id)
    if not clean_name and not norm_print_issn and not norm_electronic_issn and not openalex_id:
        return None
    name_cache_key = (clean_name.lower(), venue_type) if clean_name else None

    if norm_print_issn and norm_print_issn in cache.get("venues_by_issn", {}):
        return cache["venues_by_issn"][norm_print_issn]
    if norm_electronic_issn and norm_electronic_issn in cache.get("venues_by_eissn", {}):
        return cache["venues_by_eissn"][norm_electronic_issn]
    if name_cache_key and name_cache_key in cache.get("venues_by_name", {}):
        return cache["venues_by_name"][name_cache_key]
    if openalex_id and openalex_id in cache.get("venues_by_openalex", {}):
        return cache["venues_by_openalex"][openalex_id]

    venue_id = None
    db_issn = None
    db_eissn = None
    db_openalex_id = None
    issn_query_parts = []
    issn_params = []
    query = ""
    if openalex_id:
        cursor.execute("SELECT id, issn, eissn, openalex_id FROM venues WHERE openalex_id = ? LIMIT 1", (openalex_id,))
        if result := cursor.fetchone():
            venue_id, db_issn, db_eissn, db_openalex_id = result
    if print_issn or electronic_issn:
        if print_issn:
            issn_query_parts.append("issn = ?")
            issn_params.append(print_issn[:9])
        if electronic_issn:
            issn_query_parts.append("eissn = ?")
            issn_params.append(electronic_issn[:9])
        if issn_query_parts:
            query = "SELECT id, issn, eissn, openalex_id FROM venues WHERE " + " OR ".join(issn_query_parts) + " LIMIT 1"
            cursor.execute(query, tuple(issn_params))
            if result := cursor.fetchone():
                venue_id, db_issn, db_eissn, db_openalex_id = result

    if not venue_id and clean_name:
        cursor.execute("SELECT id, issn, eissn, openalex_id FROM venues WHERE name = ? AND type = ? LIMIT 1", (clean_name, venue_type))
        if result := cursor.fetchone():
            venue_id, db_issn, db_eissn, db_openalex_id = result

    if venue_id:
        updates = []
        update_params = []
        if print_issn and not db_issn:
            updates.append("issn = ?")
            update_params.append(print_issn[:9])
        if electronic_issn and not db_eissn:
            updates.append("eissn = ?")
            update_params.append(electronic_issn[:9])
        if openalex_id and not db_openalex_id:
            updates.append("openalex_id = ?")
            update_params.append(openalex_id)
        if updates:
            update_params.append(venue_id)
            try:
                cursor.execute("UPDATE venues SET " + ", ".join(updates) + " WHERE id = ?", tuple(update_params))
            except mariadb.Error:
                pass
    else:
        if not clean_name:
            return None
        try:
            cursor.execute(
                "INSERT INTO venues (name, type, issn, eissn, openalex_id) VALUES (?, ?, ?, ?, ?)",
                (clean_name, venue_type, print_issn[:9] if print_issn else None, electronic_issn[:9] if electronic_issn else None, openalex_id),
            )
            venue_id = cursor.lastrowid
        except mariadb.IntegrityError:
            if openalex_id:
                cursor.execute("SELECT id FROM venues WHERE openalex_id = ? LIMIT 1", (openalex_id,))
                if result := cursor.fetchone():
                    venue_id = result[0]
            if issn_query_parts:
                cursor.execute(query, tuple(issn_params))
                if result := cursor.fetchone():
                    venue_id = result[0]
            if not venue_id and clean_name:
                cursor.execute("SELECT id FROM venues WHERE name = ? AND type = ? LIMIT 1", (clean_name, venue_type))
                if result := cursor.fetchone():
                    venue_id = result[0]

    if venue_id:
        if norm_print_issn:
            cache.setdefault("venues_by_issn", {})[norm_print_issn] = venue_id
        if norm_electronic_issn:
            cache.setdefault("venues_by_eissn", {})[norm_electronic_issn] = venue_id
        if name_cache_key:
            cache.setdefault("venues_by_name", {})[name_cache_key] = venue_id
        if openalex_id:
            cache.setdefault("venues_by_openalex", {})[openalex_id] = venue_id
    return venue_id


def get_or_create_subject(cursor: mariadb.Cursor, term: str, cache: Dict[str, Dict]) -> Optional[int]:
    if not term or not (clean_term := term.strip()):
        return None
    clean_term = clean_term[:255]
    cache_key = clean_term.lower()
    if cache_key in cache.get("subjects_by_term", {}):
        return cache["subjects_by_term"][cache_key]

    term_key_simulated = normalize_term_key(cache_key)[:255]
    if not term_key_simulated:
        return None

    cursor.execute(
        "SELECT id FROM subjects WHERE vocabulary = 'KEYWORD' AND subject_type IS NULL AND term_key = ?",
        (term_key_simulated,),
    )
    if result := cursor.fetchone():
        subject_id = result[0]
    else:
        try:
            cursor.execute(
                "INSERT INTO subjects (term, vocabulary, subject_type, term_key) VALUES (?, 'KEYWORD', NULL, ?)",
                (clean_term, term_key_simulated),
            )
            subject_id = cursor.lastrowid
        except mariadb.IntegrityError:
            cursor.execute(
                "SELECT id FROM subjects WHERE vocabulary = 'KEYWORD' AND subject_type IS NULL AND term_key = ?",
                (term_key_simulated,),
            )
            if result := cursor.fetchone():
                subject_id = result[0]
            else:
                subject_id = None

    if subject_id:
        cache.setdefault("subjects_by_term", {})[cache_key] = subject_id
    return subject_id


def discover_input_folders(root_dir: str) -> List[str]:
    if not os.path.isdir(root_dir):
        logging.critical(f"Root dir '{root_dir}' not found.")
        return []
    subdirectories = sorted([d.path for d in os.scandir(root_dir) if d.is_dir()])
    return subdirectories if subdirectories else [root_dir]


def collect_json_files(root_folder: str, limit: Optional[int]) -> List[str]:
    all_json_files: List[str] = []
    for r, _, fs in os.walk(root_folder):
        for f in fs:
            if not f.endswith(".json"):
                continue
            all_json_files.append(os.path.join(r, f))
            if limit and len(all_json_files) >= limit:
                return all_json_files
    return all_json_files
