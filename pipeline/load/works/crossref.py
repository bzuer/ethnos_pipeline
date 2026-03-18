
"""
populate_from_crossref_gem_v4.1.py (Ajustado)

Versão do script Crossref (originalmente v2.7.1) atualizada para o
novo esquema do banco de dados (onde identificadores foram movidos
de 'works' para 'publications').

Modificações v4.1 (Correções de Lógica e Schema):
- **CORREÇÃO (Erro de Lógica):** Removida a lógica de `pmid`/`pmcid`
  de `sync_publication_details` (linhas 620-642 da v4.0). Essa lógica
  foi copiada incorretamente do script OpenAlex e não se aplica
  aos dados da Crossref (que não fornece esses IDs nos campos esperados).

Modificações v4.0 (Schema Refactor):
- **Atualização de `sync_work_details`**:
  - Removida a lógica de atualização para `pmid`, `pmcid`, e `url`,
    pois estes campos não existem mais na tabela `works`.
  - Mantida a lógica para `subtitle`, `abstract` e `language`.
- **Atualização de `sync_publication_details`**:
  - (Lógica de PMID/PMCID da v4.0 revertida na v4.1 por ser incorreta).

REQUISITO: O banco de dados DEVE ter a coluna `persons.normalized_name`
mantida pela função `clean_person_name` (v3.3) e seus TRIGGERS.
"""
import os
import sys
import json
import mariadb
import logging
import argparse
import resource
import re
import hashlib
from urllib.parse import urlparse, parse_qs
from typing import Dict, Optional, List, Tuple
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INGEST_DIR = os.path.dirname(SCRIPT_DIR)
if INGEST_DIR not in sys.path:
    sys.path.insert(0, INGEST_DIR)

from common import (
    LICENSE_VERSION_RANK,
    build_cache,
    clean_ingest_text,
    classify_ingest_exclusion,
    collect_json_files,
    discover_input_folders,
    ensure_connection,
    extract_first_text,
    format_iso_timestamp,
    get_connection,
    get_or_create_organization,
    get_or_create_person,
    get_or_create_subject,
    get_or_create_venue,
    normalize_doi,
    normalize_language_code,
    normalize_term_key,
    safe_rollback,
)


try:
    MEMORY_LIMIT = 8 * 1024 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))
except (ValueError, resource.error) as e:
    logging.warning(f"Não foi possível definir o limite de memória: {e}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')







STATUS_INSERTED = "INSERTED"
STATUS_UPDATED = "UPDATED"
STATUS_NO_CHANGE = "NO_CHANGE"
STATUS_SKIPPED = "SKIPPED"
STATUS_ERROR = "ERROR"

WORK_TYPE_MAP = {
    'journal-article': 'ARTICLE', 'article': 'ARTICLE', 'book-chapter': 'CHAPTER',
    'book': 'BOOK', 'proceedings-article': 'CONFERENCE', 'dataset': 'DATASET',
    'report': 'REPORT', 'thesis': 'THESIS', 'book-section': 'CHAPTER',
    'monograph': 'BOOK', 'reference-book': 'BOOK', 'proceedings': 'CONFERENCE',
    'journal-issue': 'OTHER', 'journal': 'OTHER', 'report-series': 'REPORT',
    'standard': 'OTHER', 'component': 'OTHER', 'dissertation': 'THESIS',
    'posted-content': 'OTHER', 'peer-review': 'OTHER', 'editorial': 'OTHER',
    'erratum': 'OTHER', 'grant': 'OTHER', 'letter': 'OTHER', 'review': 'OTHER',
    'paratext': 'OTHER', 'other': 'OTHER',
}
ORG_TYPE_PUBLISHER = 'PUBLISHER'
ORG_TYPE_INSTITUTE = 'INSTITUTE'
ORG_TYPE_FUNDER = 'FUNDER'
VENUE_TYPE_JOURNAL = 'JOURNAL'
VENUE_TYPE_CONFERENCE = 'CONFERENCE'
VENUE_TYPE_OTHER = 'OTHER' 


def extract_publication_date(message: Dict) -> Optional[str]:
                                                                             
    for key in ['published-print', 'published-online', 'issued', 'created']:
        date_info = message.get(key) or {}
        if date_info and isinstance(date_info, dict) and date_info.get('date-parts'):
            dp = date_info['date-parts'][0]
            if not isinstance(dp, list): continue
            try:
                year = int(dp[0])
                if not (1000 <= year <= 9999): continue
                month = 1
                if len(dp) > 1:
                    try:
                        m = int(dp[1])
                        if 1 <= m <= 12: month = m
                    except (TypeError, ValueError): pass
                day = 1
                if len(dp) > 2:
                    try:
                        d = int(dp[2])
                        if 1 <= d <= 31: day = d
                    except (TypeError, ValueError): pass
                return f"{year:04d}-{month:02d}-{day:02d}"
            except (IndexError, TypeError, ValueError):
                try:
                    year = int(dp[0])
                    if 1000 <= year <= 9999:
                        return f"{year:04d}-01-01"
                except (IndexError, TypeError, ValueError):
                    continue
    return None

def _extract_scielo_pid_from_url(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if "scielo" not in (parsed.netloc or "") and "scielo" not in (parsed.path or ""):
        return None
    query = parse_qs(parsed.query or "")
    pid_list = query.get("pid")
    if pid_list:
        pid = pid_list[0].strip()
        return pid[:50] if pid else None
    return None

def _extract_scielo_pid(message: Dict) -> Optional[str]:
    resource_url = ((message.get("resource") or {}).get("primary") or {}).get("URL")
    pid = _extract_scielo_pid_from_url(resource_url)
    if pid:
        return pid
    for link in message.get("link") or []:
        pid = _extract_scielo_pid_from_url(link.get("URL"))
        if pid:
            return pid
    return None

def _infer_file_format(url: str, content_type: Optional[str]) -> str:
    if content_type:
        lowered = content_type.lower()
        if "pdf" in lowered:
            return "PDF"
        if "epub" in lowered:
            return "EPUB"
        if "html" in lowered:
            return "HTML"
        if "xml" in lowered:
            return "XML"
        if "text" in lowered:
            return "TXT"
    lowered_url = (url or "").lower()
    if lowered_url.endswith(".pdf"):
        return "PDF"
    if lowered_url.endswith(".epub"):
        return "EPUB"
    if lowered_url.endswith(".mobi"):
        return "MOBI"
    if lowered_url.endswith(".html") or lowered_url.endswith(".htm"):
        return "HTML"
    if lowered_url.endswith(".xml"):
        return "XML"
    if lowered_url.endswith(".doc") or lowered_url.endswith(".docx"):
        return "DOCX"
    if lowered_url.endswith(".txt"):
        return "TXT"
    return "OTHER"

def _is_scielo_pdf(url: Optional[str], content_type: Optional[str]) -> bool:
    if not url or not isinstance(url, str):
        return False
    lowered_url = url.lower()
    try:
        parsed = urlparse(lowered_url)
    except Exception:
        parsed = None
    netloc = parsed.netloc if parsed else ""
    path = parsed.path if parsed else lowered_url
    if "scielo" not in (netloc or "") and "scielo" not in (path or ""):
        return False
    return _infer_file_format(lowered_url, content_type) == "PDF"

def sync_crossref_files(cursor: mariadb.Cursor, publication_id: int, message: Dict) -> bool:
    links = message.get("link") or []
    resource_url = ((message.get("resource") or {}).get("primary") or {}).get("URL")
    candidates: List[Dict[str, Optional[str]]] = []
    seen_urls = set()
    for link in links:
        url = link.get("URL")
        if not url:
            continue
        url = url.strip()
        if not url or url in seen_urls:
            continue
        if not _is_scielo_pdf(url, link.get("content-type")):
            continue
        seen_urls.add(url)
        candidates.append(
            {
                "url": url,
                "content_type": link.get("content-type"),
                "content_version": link.get("content-version"),
                "intended_application": link.get("intended-application"),
                "role": "link",
            }
        )
    if resource_url:
        resource_url = resource_url.strip()
        if resource_url and resource_url not in seen_urls and _is_scielo_pdf(resource_url, None):
            candidates.append(
                {
                    "url": resource_url,
                    "content_type": None,
                    "content_version": None,
                    "intended_application": None,
                    "role": "landing_page",
                }
            )

    if not candidates:
        return False

    changes_made = False
    for item in candidates:
        url = item["url"]
        if not url:
            continue
        file_format = _infer_file_format(url, item["content_type"])
        url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
        content_version = item["content_version"]
        if content_version:
            content_version = str(content_version)[:20]
        metadata = {
            "source": "crossref",
            "role": item["role"],
            "content_type": item["content_type"],
            "content_version": item["content_version"],
            "intended_application": item["intended_application"],
        }
        metadata_json = json.dumps(metadata)
        download_urls_json = json.dumps([{"url": url, **metadata}])
        try:
            cursor.execute(
                """
                INSERT INTO files
                (md5, publication_id, best_oa_url, file_format, verification_status, content_version, download_urls, external_metadata)
                VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    best_oa_url = VALUES(best_oa_url),
                    content_version = COALESCE(content_version, VALUES(content_version)),
                    download_urls = COALESCE(download_urls, VALUES(download_urls)),
                    external_metadata = COALESCE(external_metadata, VALUES(external_metadata))
                """,
                (url_hash, publication_id, url, file_format, content_version, download_urls_json, metadata_json),
            )
            file_changed = cursor.rowcount > 0
            cursor.execute("SELECT id FROM files WHERE md5 = ? AND publication_id = ?", (url_hash, publication_id))
            result = cursor.fetchone()
            if not result:
                continue
            if file_changed:
                changes_made = True
        except mariadb.Error as e:
            logging.error(f"PUB {publication_id} error=crossref-file detail={e}")
    return changes_made

def parse_crossref_file(filepath: str) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if data.get('status') in ['filtered', 'not-found']:
            return None, None, STATUS_SKIPPED
        message = data.get('message') or data.get('crossref_response', {}).get('message', {})
        if not message:
            return None, None, STATUS_SKIPPED
        doi = normalize_doi(message.get("DOI"))
        if not doi:
            return None, None, STATUS_SKIPPED
        return message, doi, None
    except (json.JSONDecodeError, KeyError, AttributeError, FileNotFoundError) as e:
        logging.warning(f"FILE {os.path.basename(filepath)} error={type(e).__name__}")
        return None, None, STATUS_ERROR
    except Exception as e:
        logging.error(f"FILE {os.path.basename(filepath)} error={e}")
        return None, None, STATUS_ERROR



def get_crossref_exclusion_reason(message: Optional[Dict]) -> Optional[str]:
    if not message:
        return None
    title = clean_ingest_text(extract_first_text(message.get("title")))
    if not title:
        return "missing-title"
    return classify_ingest_exclusion(
        title,
        message.get("type"),
        source="crossref",
    )


def should_skip_existing_doi(
    cursor: mariadb.Cursor,
    doi: Optional[str],
    mode: str,
    doi_presence_cache: Dict[str, bool],
) -> bool:
    if mode != "new" or not doi:
        return False
    if doi in doi_presence_cache:
        return doi_presence_cache[doi]
    cursor.execute("SELECT 1 FROM publications WHERE doi = ? LIMIT 1", (doi,))
    exists = cursor.fetchone() is not None
    doi_presence_cache[doi] = exists
    return exists


def mark_doi_processed(
    doi: Optional[str],
    mode: str,
    status: str,
    doi_presence_cache: Dict[str, bool],
) -> None:
    if mode == "new" and doi and status in (STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE):
        doi_presence_cache[doi] = True


def sync_work_details(cursor: mariadb.Cursor, work_id: int, message: Dict) -> bool:
                                                        
    try:
        cursor.execute("SELECT subtitle, abstract, language FROM works WHERE id = ?", (work_id,))
        current_work = cursor.fetchone()
        if not current_work: 
            logging.error(f"WORK {work_id} error=not_found")
            return False

        updates, params = [], []
        json_subtitle = clean_ingest_text(extract_first_text(message.get('subtitle')))
        json_abstract = _extract_crossref_abstract(message)
        json_lang = normalize_language_code(message.get('language'))

        if json_subtitle and not current_work[0]:
            updates.append("subtitle = ?"); params.append(json_subtitle[:65535])
        if json_abstract and not current_work[1]:
            updates.append("abstract = ?"); params.append(json_abstract)
        if json_lang and not current_work[2]:
            updates.append("language = ?"); params.append(json_lang)
        
        if updates: 
            params.append(work_id)
            cursor.execute(f"UPDATE works SET {', '.join(updates)} WHERE id = ?", tuple(params))
            return True
        return False
    except mariadb.Error as e: 
        logging.error(f"WORK {work_id} error=sql detail={e}")
        return False
    except Exception as e: 
        logging.error(f"WORK {work_id} error=unexpected detail={e}")
        return False

def sync_publication_details(cursor: mariadb.Cursor, publication_id: int, message: Dict, cache: Dict[str, Dict]) -> bool:
                                                               
    try:
        cursor.execute("""
            SELECT
                venue_id, publisher_id, publication_date, volume, issue, pages, source, isbn,
                source_indexed_at, source_deposited_at, source_prefix, source_member_id, scielo_pid, open_access
            FROM publications WHERE id = ?
        """, (publication_id,))
        current_pub = cursor.fetchone()
        if not current_pub: 
            logging.error(f"PUB {publication_id} error=not_found")
            return False

        pub_updates, pub_params = [], []
        work_type = WORK_TYPE_MAP.get(message.get('type'), 'OTHER')
        
        indexed_data = message.get('indexed') or {}
        deposited_data = message.get('deposited') or {}

        
        if not current_pub[0]: 
            venue_name = clean_ingest_text(extract_first_text(message.get('container-title')))
            print_issn, e_issn = None, None; issn_list = message.get('ISSN', []); issn_type_list = message.get('issn-type', [])
            for item in (issn_type_list or []):
                 if item.get('type') == 'print': print_issn = item.get('value')
                 elif item.get('type') == 'electronic': e_issn = item.get('value')
            if not print_issn and not e_issn and issn_list:
                 if len(issn_list) > 0: print_issn = issn_list[0];
                 if len(issn_list) > 1: e_issn = issn_list[1]
            venue_type = VENUE_TYPE_CONFERENCE if work_type == 'CONFERENCE' else VENUE_TYPE_JOURNAL
            if venue_id := get_or_create_venue(cursor, venue_name, print_issn, e_issn, venue_type, cache):
                pub_updates.append("venue_id = ?"); pub_params.append(venue_id)
        
        if not current_pub[1]: 
            publisher_name = clean_ingest_text(message.get('publisher'))
            if publisher_id := get_or_create_organization(cursor, publisher_name, ORG_TYPE_PUBLISHER, cache):
                pub_updates.append("publisher_id = ?"); pub_params.append(publisher_id)
        
        if not current_pub[2] and (pub_date := extract_publication_date(message)): 
            pub_updates.append("publication_date = ?"); pub_params.append(pub_date)
        if not current_pub[3] and (volume := message.get('volume')): 
            pub_updates.append("volume = ?"); pub_params.append(str(volume)[:50])
        if not current_pub[4] and (issue := message.get('issue')): 
            pub_updates.append("issue = ?"); pub_params.append(str(issue)[:50])
        if not current_pub[5] and (pages := message.get('page')): 
            pub_updates.append("pages = ?"); pub_params.append(str(pages)[:255])
        if not current_pub[7] and (isbn_list := message.get('ISBN', [])) and isinstance(isbn_list, list) and len(isbn_list) > 0: 
            pub_updates.append("isbn = ?"); pub_params.append(str(isbn_list[0])[:20])
        if not current_pub[6] and (source := message.get('source')): 
            pub_updates.append("source = ?"); pub_params.append(source[:50])
        if not current_pub[8] and (idx_raw := indexed_data.get('date-time')) and (ft := format_iso_timestamp(idx_raw)): 
            pub_updates.append("source_indexed_at = ?"); pub_params.append(ft)
        if not current_pub[9] and (dep_raw := deposited_data.get('date-time')) and (ft := format_iso_timestamp(dep_raw)): 
            pub_updates.append("source_deposited_at = ?"); pub_params.append(ft)
        if not current_pub[10] and (prefix := message.get('prefix')): 
            pub_updates.append("source_prefix = ?"); pub_params.append(prefix[:50])
        if not current_pub[11] and (member := message.get('member')): 
            pub_updates.append("source_member_id = ?"); pub_params.append(member[:50])
        if not current_pub[12] and (scielo_pid := _extract_scielo_pid(message)):
            cursor.execute("SELECT id FROM publications WHERE scielo_pid = ? AND id != ?", (scielo_pid, publication_id))
            if not cursor.fetchone():
                pub_updates.append("scielo_pid = ?"); pub_params.append(scielo_pid)
                if current_pub[13] == 0:
                    pub_updates.append("open_access = 1")
        
        if pub_updates: 
            pub_params.append(publication_id)
            cursor.execute(f"UPDATE publications SET {', '.join(pub_updates)} WHERE id = ?", tuple(pub_params))
            return True
        return False
    except mariadb.Error as e: 
        logging.error(f"PUB {publication_id} error=sql detail={e}")
        return False
    except Exception as e: 
        logging.error(f"PUB {publication_id} error=unexpected detail={e}")
        return False

def sync_authorships(cursor: mariadb.Cursor, work_id: int, authors_from_json: List[Dict], cache: Dict[str, Dict]) -> bool:
                                      
    if not authors_from_json: return False
    changes_made = False
    try: 
        cursor.execute("SELECT person_id, position FROM authorships WHERE work_id = ? AND role = 'AUTHOR'", (work_id,))
        current_authors = {row[0]: row[1] for row in cursor.fetchall()}
    except mariadb.Error as e: 
        logging.error(f"WORK {work_id} error=authorships-select detail={e}")
        return False

    authors_to_add = []; authors_to_update = []
    processed_person_ids = set()
    duplicate_author_entries = 0
    for i, author_data in enumerate(authors_from_json, 1):
        person_id = get_or_create_person(cursor, author_data, cache)
        if not person_id: continue
        if person_id in processed_person_ids:
            duplicate_author_entries += 1
            continue
        processed_person_ids.add(person_id)

        affiliation_name = None
        aff_list = author_data.get('affiliation') or []

        if isinstance(aff_list, list) and aff_list and isinstance(aff_list[0], dict):
             affiliation_name = aff_list[0].get('name')

        affiliation_id = get_or_create_organization(cursor, affiliation_name, ORG_TYPE_INSTITUTE, cache)
        is_corresponding = 0

        if person_id not in current_authors:
            authors_to_add.append((work_id, person_id, affiliation_id, i, is_corresponding))
        elif current_authors[person_id] != i:
            authors_to_update.append((affiliation_id, i, is_corresponding, work_id, person_id))

    is_partial = bool(current_authors) and len(processed_person_ids) < len(current_authors)
    if is_partial and authors_to_update:
        logging.debug(
            f"WORK {work_id}: skipped {len(authors_to_update)} authorship reorders from partial payload "
            f"(payload={len(processed_person_ids)}, db={len(current_authors)})"
        )
        authors_to_update = []

    if authors_to_add:
        try:
            cursor.executemany("INSERT IGNORE INTO authorships (work_id, person_id, affiliation_id, position, is_corresponding, role) VALUES (?, ?, ?, ?, ?, 'AUTHOR')", authors_to_add)
            changes_made = changes_made or cursor.rowcount > 0
            if cursor.rowcount > 0:
                logging.info(f"WORK {work_id}: inserted {cursor.rowcount}/{len(authors_to_add)} authorships")
        except mariadb.Error as e: logging.error(f"WORK {work_id} error=authorships-insert detail={e}")
    if authors_to_update:
         try:
             cursor.executemany("""UPDATE authorships SET affiliation_id = ?, position = ?, is_corresponding = ?
                                   WHERE work_id = ? AND person_id = ? AND role = 'AUTHOR'""", authors_to_update)
             changes_made = changes_made or cursor.rowcount > 0
             if cursor.rowcount > 0:
                 logging.info(f"WORK {work_id}: updated {cursor.rowcount}/{len(authors_to_update)} authorships")
         except mariadb.Error as e: logging.error(f"WORK {work_id} error=authorships-update detail={e}")
    if duplicate_author_entries:
        logging.debug(f"WORK {work_id}: skipped {duplicate_author_entries} duplicate author entries (same person_id)")
    if changes_made:
         try: 
             cursor.callproc('sp_update_work_author_summary', (work_id,))
         except mariadb.Error as e: 
             logging.error(f"WORK {work_id} error=authorships-summary detail={e}")
    return changes_made

def sync_funding(cursor: mariadb.Cursor, work_id: int, funders_from_json: List[Dict], cache: Dict[str, Dict]) -> bool:
                                                        
    if not funders_from_json: return False
    try: 
        cursor.execute("SELECT funder_id, grant_number FROM funding WHERE work_id = ?", (work_id,))
        existing_funding = {(row[0], row[1]) for row in cursor.fetchall()}
    except mariadb.Error as e: 
        logging.error(f"WORK {work_id} error=funding-select detail={e}")
        return False
    funders_to_add = []
    for funder_data in funders_from_json:
        funder_id = get_or_create_organization(cursor, funder_data.get('name'), ORG_TYPE_FUNDER, cache)
        if not funder_id: continue
        grant_number = None
        if awards := (funder_data.get('award') or []):
             if isinstance(awards, list) and awards and isinstance(awards[0], str): 
                 grant_number = awards[0].strip()[:100]
        if (funder_id, grant_number) not in existing_funding: 
            funders_to_add.append((work_id, funder_id, grant_number))
            existing_funding.add((funder_id, grant_number))
    if funders_to_add:
        try:
            cursor.executemany("INSERT IGNORE INTO funding (work_id, funder_id, grant_number) VALUES (?, ?, ?)", funders_to_add)
            if cursor.rowcount > 0:
                logging.info(f"WORK {work_id}: inserted {cursor.rowcount}/{len(funders_to_add)} funding rows")
            return cursor.rowcount > 0
        except mariadb.Error as e:
            logging.error(f"WORK {work_id} error=funding-insert detail={e}")
    return False

def sync_licenses(cursor: mariadb.Cursor, publication_id: int, licenses_from_json: List[Dict]) -> bool:
                                                           
    if not licenses_from_json: return False
    best_url = None
    best_version = None
    best_date = None
    for license_data in licenses_from_json:
        url = license_data.get('URL')
        if not url:
            continue
        url = url.strip()[:512]
        content_version = str(license_data.get('content-version', '')).strip()
        if content_version:
            content_version = content_version[:50]
        else:
            content_version = None
        start_date_parts = (license_data.get('start') or {}).get('date-parts')
        date_tuple = None
        if start_date_parts and isinstance(start_date_parts, list) and len(start_date_parts) > 0 and isinstance(start_date_parts[0], list):
            try:
                y = int(start_date_parts[0][0]); m = 1; d = 1
                if len(start_date_parts[0]) > 1: m = int(start_date_parts[0][1])
                if len(start_date_parts[0]) > 2: d = int(start_date_parts[0][2])
                if 1000 <= y <= 9999 and 1 <= m <= 12 and 1 <= d <= 31:
                    date_tuple = (y, m, d)
            except (IndexError, TypeError, ValueError):
                date_tuple = None
        if best_url is None or (date_tuple and (best_date is None or date_tuple > best_date)):
            best_url = url
            best_version = content_version
            best_date = date_tuple

    if not best_url and not best_version:
        return False
    try:
        cursor.execute("SELECT license_url, license_version FROM publications WHERE id = ?", (publication_id,))
        row = cursor.fetchone()
    except mariadb.Error as e:
        logging.error(f"PUBLICATION {publication_id} error=licenses-select detail={e}")
        return False
    if not row:
        return False
    current_url, current_version = row

    # URL: only fill if empty
    new_url = current_url
    if best_url and not current_url:
        new_url = best_url

    # Version: monotonic upgrade only (never downgrade)
    new_version = current_version
    if best_version and not current_version:
        new_version = best_version
    elif best_version and current_version:
        current_rank = LICENSE_VERSION_RANK.get(str(current_version).strip().lower(), 0)
        incoming_rank = LICENSE_VERSION_RANK.get(str(best_version).strip().lower(), 0)
        if incoming_rank >= current_rank:
            new_version = best_version

    if new_url == current_url and new_version == current_version:
        return False
    try:
        cursor.execute(
            "UPDATE publications SET license_url = ?, license_version = ? WHERE id = ?",
            (new_url, new_version, publication_id),
        )
        if cursor.rowcount > 0:
            logging.info(f"PUBLICATION {publication_id}: updated license")
        return cursor.rowcount > 0
    except mariadb.Error as e:
        logging.error(f"PUBLICATION {publication_id} error=licenses-update detail={e}")
        return False

def sync_subjects(cursor: mariadb.Cursor, work_id: int, subjects_from_json: List[str], cache: Dict[str, Dict]) -> bool:
                                                 
    if not subjects_from_json: return False
    try: 
        cursor.execute("SELECT s.term_key FROM work_subjects ws JOIN subjects s ON ws.subject_id = s.id WHERE ws.work_id = ? AND s.vocabulary = 'KEYWORD'", (work_id,))
        existing_term_keys = {row[0] for row in cursor.fetchall()}
    except mariadb.Error as e: 
        logging.error(f"WORK {work_id} error=subjects-select detail={e}")
        return False
    
    subjects_to_add = []; processed_term_keys_in_batch = set()
    for term in subjects_from_json:
        if not term or not isinstance(term, str): continue
        
        term_key = normalize_term_key(term.strip().lower()[:255])
        
        if not term_key or term_key in existing_term_keys or term_key in processed_term_keys_in_batch: 
            continue
        
        subject_id = get_or_create_subject(cursor, term, cache)
        
        if subject_id: 
            subjects_to_add.append((work_id, subject_id))
            existing_term_keys.add(term_key) 
            processed_term_keys_in_batch.add(term_key)
            
    if subjects_to_add:
        try:
            cursor.executemany("INSERT IGNORE INTO work_subjects (work_id, subject_id) VALUES (?, ?)", subjects_to_add)
            if cursor.rowcount > 0:
                try:
                    cursor.callproc('sp_update_work_subjects_summary', (work_id,))
                except mariadb.Error as e_sp:
                    logging.error(f"WORK {work_id} error=subjects-summary detail={e_sp}")
                logging.info(f"WORK {work_id}: linked {cursor.rowcount}/{len(subjects_to_add)} subjects")
                return True
        except mariadb.Error as e:
            logging.error(f"WORK {work_id} error=subjects-insert detail={e}")
    return False

def sync_citations(cursor: mariadb.Cursor, work_id: int, references: List[Dict]) -> bool:
                                                                 
    if not references: return False
    unique_cited_dois = {
        norm_doi
        for ref in references
        for norm_doi in [normalize_doi(ref.get("DOI"))]
        if norm_doi
    }
    if not unique_cited_dois: return False
    try:
        batch_size = 1000
        dois_list = list(unique_cited_dois)
        existing_dois = set()
        for i in range(0, len(dois_list), batch_size):
            batch_dois = dois_list[i:i + batch_size]
            placeholders = ', '.join(['?'] * len(batch_dois))
            params = (work_id, *batch_dois)
            cursor.execute(
                f"SELECT cited_doi FROM work_references WHERE citing_work_id = ? AND cited_doi IN ({placeholders})",
                params,
            )
            for row in cursor.fetchall():
                existing_norm = normalize_doi(row[0])
                if existing_norm:
                    existing_dois.add(existing_norm)

        refs_to_add = [(work_id, doi[:255]) for doi in unique_cited_dois if doi not in existing_dois]
        if not refs_to_add:
            return False

        cursor.executemany(
            "INSERT IGNORE INTO work_references (citing_work_id, cited_doi) VALUES (?, ?)",
            refs_to_add,
        )
        cursor.execute("SELECT ROW_COUNT()")
        inserted_count = int(cursor.fetchone()[0] or 0)
        if inserted_count > 0:
            logging.info(f"WORK {work_id}: inserted {inserted_count}/{len(refs_to_add)} references")
        return inserted_count > 0
    except mariadb.Error as e: 
        logging.error(f"WORK {work_id} error=references-sql detail={e}")
        return False
    except Exception as e: 
        logging.error(f"WORK {work_id} error=references-unexpected detail={e}")
        return False


def _extract_crossref_abstract(message: Dict) -> Optional[str]:
    json_abstract = message.get("abstract")
    if not json_abstract:
        return None
    cleaned = clean_ingest_text(re.sub(r"<jats:sec>.*?</jats:sec>", "", json_abstract, flags=re.DOTALL))
    return cleaned[:16777215] if cleaned else None


def _process_record_no_tx(
    cursor: mariadb.Cursor,
    filepath: str,
    message: Optional[Dict],
    doi: Optional[str],
    mode: str,
    cache: Dict[str, Dict],
) -> Tuple[str, Optional[int], Optional[int], Optional[str]]:
    if not message or not doi:
        return STATUS_SKIPPED, None, None, doi

    title_list = message.get("title", [])
    clean_title = clean_ingest_text(extract_first_text(title_list))
    if not clean_title:
        return STATUS_SKIPPED, None, None, doi

    cursor.execute(
        "SELECT p.work_id, p.id, w.title, w.work_type, w.abstract FROM publications p JOIN works w ON p.work_id = w.id WHERE p.doi = ?",
        (doi,),
    )
    result = cursor.fetchone()
    if result and mode == "new":
        return STATUS_SKIPPED, None, None, doi

    if result:
        work_id, publication_id, current_title, current_work_type, current_abstract = result
        json_title = clean_ingest_text(extract_first_text(title_list))
        if json_title:
            json_title = json_title[:65535]
        work_type_str = message.get("type", "other").lower()
        json_work_type = WORK_TYPE_MAP.get(work_type_str, "OTHER")
        json_abstract = _extract_crossref_abstract(message)

        updates = []
        params = []
        data_changed = False
        if (not current_title or current_title == "Título Indisponível") and json_title:
            updates.append("title = ?")
            params.append(json_title)
        if (not current_work_type or current_work_type == "OTHER") and (json_work_type and json_work_type != "OTHER"):
            updates.append("work_type = ?")
            params.append(json_work_type)
        if json_abstract and not current_abstract:
            updates.append("abstract = ?")
            params.append(json_abstract)

        if updates:
            params.append(work_id)
            cursor.execute(f"UPDATE works SET {', '.join(updates)} WHERE id = ?", tuple(params))
            data_changed = data_changed or cursor.rowcount > 0

        changes = [data_changed]
        changes.append(sync_work_details(cursor, work_id, message))
        changes.append(sync_publication_details(cursor, publication_id, message, cache))
        changes.append(sync_authorships(cursor, work_id, message.get("author") or [], cache))
        changes.append(sync_citations(cursor, work_id, message.get("reference") or []))
        changes.append(sync_funding(cursor, work_id, message.get("funder") or [], cache))
        changes.append(sync_licenses(cursor, publication_id, message.get("license") or []))
        changes.append(sync_subjects(cursor, work_id, message.get("subject") or [], cache))
        changes.append(sync_crossref_files(cursor, publication_id, message))

        status = STATUS_UPDATED if any(changes) else STATUS_NO_CHANGE
        if any(changes):
            logging.info(
                f"FILE {os.path.basename(filepath)} status={status} doi={doi} openalex_id=- work_id={work_id} publication_id={publication_id}"
            )
        return status, work_id, publication_id, doi

    title = clean_title[:65535]
    work_type_str = message.get("type", "other").lower()
    work_type_enum = WORK_TYPE_MAP.get(work_type_str, "OTHER")
    json_abstract = _extract_crossref_abstract(message)
    json_language = normalize_language_code(message.get('language'))

    cursor.execute("INSERT INTO works (title, work_type, abstract, language) VALUES (?, ?, ?, ?)", (title, work_type_enum, json_abstract, json_language))
    work_id = cursor.lastrowid
    scielo_pid = _extract_scielo_pid(message)
    if scielo_pid:
        cursor.execute("SELECT id FROM publications WHERE scielo_pid = ?", (scielo_pid,))
        if cursor.fetchone():
            scielo_pid = None
    if scielo_pid:
        cursor.execute(
            "INSERT INTO publications (work_id, doi, scielo_pid) VALUES (?, ?, ?)",
            (work_id, doi[:255], scielo_pid),
        )
    else:
        cursor.execute("INSERT INTO publications (work_id, doi) VALUES (?, ?)", (work_id, doi[:255]))
    publication_id = cursor.lastrowid

    sync_work_details(cursor, work_id, message)
    sync_publication_details(cursor, publication_id, message, cache)
    sync_authorships(cursor, work_id, message.get("author") or [], cache)
    sync_citations(cursor, work_id, message.get("reference") or [])
    sync_funding(cursor, work_id, message.get("funder") or [], cache)
    sync_licenses(cursor, publication_id, message.get("license") or [])
    sync_subjects(cursor, work_id, message.get("subject") or [], cache)
    sync_crossref_files(cursor, publication_id, message)

    logging.info(
        f"FILE {os.path.basename(filepath)} status={STATUS_INSERTED} doi={doi} openalex_id=- work_id={work_id} publication_id={publication_id}"
    )
    return STATUS_INSERTED, work_id, publication_id, doi


def process_record(
    conn: mariadb.Connection,
    filepath: str,
    message: Optional[Dict],
    doi: Optional[str],
    mode: str,
    cache: Dict[str, Dict],
) -> str:
    cursor = None
    try:
        cursor = conn.cursor()
        conn.begin()
        status, _, _, _ = _process_record_no_tx(cursor, filepath, message, doi, mode, cache)
        if status == STATUS_ERROR:
            safe_rollback(conn, f"FILE {os.path.basename(filepath)}")
        else:
            conn.commit()
        return status
    except mariadb.IntegrityError as ie:
        safe_rollback(conn, f"FILE {os.path.basename(filepath)}")
        logging.error(f"FILE {os.path.basename(filepath)} error=IntegrityError detail={ie}")
        return STATUS_ERROR
    except (mariadb.Error, Exception) as e:
        safe_rollback(conn, f"FILE {os.path.basename(filepath)}")
        logging.error(f"FILE {os.path.basename(filepath)} error={e}")
        return STATUS_ERROR
    finally:
        if cursor:
            try:
                cursor.close()
            except mariadb.Error:
                pass


def main(args):
    conn = None
    total_processed_files = 0
    total_inserted = 0
    total_updated = 0
    total_validated = 0
    total_skipped = 0
    total_errors = 0
    script_start_time = datetime.now()
    try:
        root_dir = args.directory
        subdirectories = discover_input_folders(root_dir)
        if not subdirectories:
            return

        conn = get_connection(args.config)
        logging.info(f"Ingest mode: {args.mode}")
        doi_presence_cache: Dict[str, bool] = {}

        cache_reset_interval = args.cache_reset_interval or 0

        for subdir_path in subdirectories:
            folder_name = os.path.basename(subdir_path)
            cache = build_cache()
            all_json_files = collect_json_files(subdir_path, args.limit)
            total_files_in_folder = len(all_json_files)
            logging.info(f"Processing {total_files_in_folder} files in {folder_name}...")
            if args.commit_batch and args.commit_batch > 0:
                folder_commit_batch = max(1, min(args.commit_batch, total_files_in_folder or 1))
                logging.info(f"COMMIT mode=fixed folder={folder_name} batch={folder_commit_batch}")
            else:
                folder_commit_batch = max(1, total_files_in_folder)
                logging.info(f"COMMIT mode=per-folder folder={folder_name} batch={folder_commit_batch}")

            if folder_commit_batch > 1:
                batch_count = 0
                conn.begin()
                cursor = conn.cursor()
                for k, filepath in enumerate(all_json_files, 1):
                    if k % 500 == 0:
                        logging.info(f"FILE progress {k}/{total_files_in_folder}")
                    if cache_reset_interval and k % cache_reset_interval == 0:
                        cache = build_cache()
                        logging.info(f"CACHE reset interval={cache_reset_interval} folder={folder_name}")

                    message, doi, parse_status = parse_crossref_file(filepath)
                    if parse_status:
                        if parse_status == STATUS_SKIPPED:
                            total_skipped += 1
                        else:
                            total_errors += 1
                        continue
                    if get_crossref_exclusion_reason(message):
                        total_skipped += 1
                        continue
                    if should_skip_existing_doi(cursor, doi, args.mode, doi_presence_cache):
                        total_skipped += 1
                        continue

                    was_open = conn.open
                    savepoint_name = f"sp_{k}"
                    force_reconnect = False
                    transaction_reset = False
                    try:
                        cursor.execute(f"SAVEPOINT {savepoint_name}")
                        status, _, _, _ = _process_record_no_tx(cursor, filepath, message, doi, args.mode, cache)
                        if status == STATUS_ERROR:
                            try:
                                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                            except mariadb.Error as e:
                                logging.error(f"FILE {os.path.basename(filepath)} rollback failed: {e}")
                                if safe_rollback(conn, f"FILE {os.path.basename(filepath)}"):
                                    transaction_reset = True
                                else:
                                    force_reconnect = True
                        else:
                            cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    except mariadb.IntegrityError as ie:
                        try:
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        except mariadb.Error as e:
                            logging.error(f"FILE {os.path.basename(filepath)} rollback failed: {e}")
                            if safe_rollback(conn, f"FILE {os.path.basename(filepath)}"):
                                transaction_reset = True
                            else:
                                force_reconnect = True
                        logging.error(f"FILE {os.path.basename(filepath)} error=IntegrityError detail={ie}")
                        status = STATUS_ERROR
                    except (mariadb.Error, Exception) as e:
                        try:
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        except mariadb.Error as re_err:
                            logging.error(f"FILE {os.path.basename(filepath)} rollback failed: {re_err}")
                            if safe_rollback(conn, f"FILE {os.path.basename(filepath)}"):
                                transaction_reset = True
                            else:
                                force_reconnect = True
                        logging.error(f"FILE {os.path.basename(filepath)} error={e}")
                        status = STATUS_ERROR

                    if force_reconnect or transaction_reset or (not conn.open and was_open):
                        try:
                            cursor.close()
                        except mariadb.Error:
                            pass
                        conn = ensure_connection(conn, args.config)
                        conn.begin()
                        cursor = conn.cursor()
                        if force_reconnect or transaction_reset:
                            batch_count = 0

                    batch_count += 1
                    if batch_count >= folder_commit_batch:
                        conn.commit()
                        conn.begin()
                        batch_count = 0

                    if status == STATUS_INSERTED:
                        total_inserted += 1
                    elif status == STATUS_UPDATED:
                        total_updated += 1
                    elif status == STATUS_NO_CHANGE:
                        total_validated += 1
                    elif status == STATUS_SKIPPED:
                        total_skipped += 1
                    elif status == STATUS_ERROR:
                        total_errors += 1
                    mark_doi_processed(doi, args.mode, status, doi_presence_cache)

                if batch_count > 0:
                    conn.commit()
                try:
                    cursor.close()
                except mariadb.Error:
                    pass
            else:
                check_cursor = conn.cursor()
                for k, filepath in enumerate(all_json_files, 1):
                    if k % 500 == 0:
                        logging.info(f"FILE progress {k}/{total_files_in_folder}")
                    if cache_reset_interval and k % cache_reset_interval == 0:
                        cache = build_cache()
                        logging.info(f"CACHE reset interval={cache_reset_interval} folder={folder_name}")

                    message, doi, parse_status = parse_crossref_file(filepath)
                    if parse_status:
                        if parse_status == STATUS_SKIPPED:
                            total_skipped += 1
                        else:
                            total_errors += 1
                        continue
                    if get_crossref_exclusion_reason(message):
                        total_skipped += 1
                        continue
                    if should_skip_existing_doi(check_cursor, doi, args.mode, doi_presence_cache):
                        total_skipped += 1
                        continue

                    was_open = conn.open
                    status = process_record(conn, filepath, message, doi, args.mode, cache)
                    if not conn.open and was_open:
                        conn = ensure_connection(conn, args.config)
                        try:
                            check_cursor.close()
                        except mariadb.Error:
                            pass
                        check_cursor = conn.cursor()

                    if status == STATUS_INSERTED:
                        total_inserted += 1
                    elif status == STATUS_UPDATED:
                        total_updated += 1
                    elif status == STATUS_NO_CHANGE:
                        total_validated += 1
                    elif status == STATUS_SKIPPED:
                        total_skipped += 1
                    elif status == STATUS_ERROR:
                        total_errors += 1
                    mark_doi_processed(doi, args.mode, status, doi_presence_cache)
                try:
                    check_cursor.close()
                except mariadb.Error:
                    pass

            total_processed_files += total_files_in_folder
            cache = build_cache()
            logging.info(f"CACHE reset (end) folder={folder_name}")

    except Exception as e:
        logging.critical(f"Critical main execution failure: {e}", exc_info=True)
    finally:
        if conn and conn.open:
            conn.close()
        logging.info(
            f"Total time: {(datetime.now() - script_start_time).total_seconds():.2f} sec. "
            f"Processed {total_processed_files} files."
        )
        logging.info(
            "Summary: "
            f"Inserted={total_inserted}, Updated={total_updated}, Validated={total_validated}, "
            f"Skipped={total_skipped}, Errors={total_errors}"
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Ingest records from Crossref JSON files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("directory", help="Root directory with subfolders (or the directory itself) containing .json files.")
    parser.add_argument("--mode", choices=["new", "full"], default="new", help="Ingest mode: only new DOIs or all records.")
    parser.add_argument("--limit", type=int, help="Limits the total number of records (new + existing) processed per folder.")
    parser.add_argument("--commit-batch", type=int, default=0, help="Commit every N files. Use 0 to commit once per subfolder.")
    parser.add_argument("--cache-reset-interval", type=int, default=5000, help="Reset in-memory cache every N files (0 to disable).")
    parser.add_argument("--config", type=str, help="Path to config.ini (defaults to script dir or repo root).")
    args = parser.parse_args()
    main(args)
