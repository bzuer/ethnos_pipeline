
"""
Load OpenAlex work metadata from JSON files into the database.

Input:  works/openalex/{issn}/{doi}.json  (raw OpenAlex work format)
Output: works, publications, authorships, funding, work_subjects, work_references, files tables.
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
from typing import Dict, Optional, List, Tuple
from datetime import datetime

from pipeline.load.db import safe_rollback
from pipeline.load.normalize import (
    normalize_doi, normalize_language_code, normalize_term_key,
    normalize_wikidata_id, clean_ingest_text, format_iso_timestamp,
)
from pipeline.load.filtering import classify_ingest_exclusion
from pipeline.load.entities import (
    get_or_create_organization, get_or_create_person,
    get_or_create_subject, get_or_create_venue,
)
from pipeline.load.constants import (
    STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE,
    STATUS_SKIPPED, STATUS_ERROR, LICENSE_VERSION_RANK,
    ORG_TYPE_PUBLISHER, ORG_TYPE_INSTITUTE, ORG_TYPE_FUNDER,
    VENUE_TYPE_JOURNAL, VENUE_TYPE_CONFERENCE, VENUE_TYPE_BOOK_SERIES,
    VENUE_TYPE_REPOSITORY, VENUE_TYPE_OTHER,
)
from pipeline.load.runner import run_work_loader, add_work_loader_arguments
from pipeline.load.works.shared import (
    confirm_author_count,
    reconcile_resolved_authorships,
)


try:
    MEMORY_LIMIT = 8 * 1024 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))
except (ValueError, resource.error) as e:
    logging.warning(f"Could not set memory limit: {e}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')







WORK_TYPE_MAP = {
    'article': 'ARTICLE', 'book-chapter': 'CHAPTER', 'book': 'BOOK',
    'proceedings-article': 'CONFERENCE', 'dataset': 'DATASET', 'report': 'REPORT',
    'dissertation': 'THESIS', 'thesis': 'THESIS', 'book-section': 'CHAPTER',
    'monograph': 'BOOK', 'reference-book': 'BOOK', 'proceedings': 'CONFERENCE',
    'journal-issue': 'OTHER', 'journal': 'OTHER', 'report-series': 'REPORT',
    'standard': 'OTHER', 'component': 'OTHER', 'posted-content': 'OTHER',
    'peer-review': 'OTHER', 'paratext': 'OTHER', 'editorial': 'OTHER',
    'erratum': 'OTHER', 'grant': 'OTHER', 'letter': 'OTHER', 'review': 'OTHER',
    'other': 'OTHER'
}
VENUE_TYPE_MAP = {
    'journal': VENUE_TYPE_JOURNAL, 'conference': VENUE_TYPE_CONFERENCE,
    'book_series': VENUE_TYPE_BOOK_SERIES, 'repository': VENUE_TYPE_REPOSITORY,
    'other': VENUE_TYPE_OTHER
}



def reconstruct_abstract(inverted_index: Optional[Dict[str, List[int]]]) -> Optional[str]:
    if not inverted_index or not isinstance(inverted_index, dict):
        return None
    try:
        reversed_map: Dict[int, str] = {}
        max_pos = -1
        for word, positions in inverted_index.items():
            if not isinstance(positions, list): continue
            for pos in positions:
                if not isinstance(pos, int): continue
                reversed_map[pos] = word
                if pos > max_pos:
                    max_pos = pos
        
        if max_pos == -1: return None
        abstract_words = [reversed_map.get(i, '') for i in range(max_pos + 1)]
        return " ".join(abstract_words).strip()
    except Exception as e:
        logging.warning(f"WORK abstract_reconstruct_failed detail={e}")
        return None

def extract_publication_date(message: Dict) -> Optional[str]:
    pub_date = message.get('publication_date')
    if pub_date and isinstance(pub_date, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', pub_date):
        try:
            year, month, day = map(int, pub_date.split('-'))
            if 1000 <= year <= 9999 and 1 <= month <= 12 and 1 <= day <= 31:
                return pub_date
        except ValueError: pass
    pub_year = message.get('publication_year')
    if pub_year and isinstance(pub_year, int):
        if 1000 <= pub_year <= 9999:
            return f"{pub_year:04d}-01-01"
    return None

def parse_openalex_file(filepath: str) -> Tuple[Optional[Dict], Optional[str], Optional[str], Optional[str]]:
    openalex_id: Optional[str] = None
    doi: Optional[str] = None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            message = json.load(f)
        if not message:
            return None, None, None, STATUS_SKIPPED

        openalex_id_url = message.get('id')
        if openalex_id_url and isinstance(openalex_id_url, str) and 'openalex.org/' in openalex_id_url:
            openalex_id = openalex_id_url.split('/')[-1]

        doi = normalize_doi(message.get('doi'))

        if not openalex_id and not doi:
            logging.warning(f"FILE {os.path.basename(filepath)} warning=no-identifiers")
            return None, None, None, STATUS_SKIPPED

        return message, openalex_id, doi, None
    except (json.JSONDecodeError, KeyError, AttributeError, FileNotFoundError) as e:
        logging.warning(f"FILE {os.path.basename(filepath)} error={type(e).__name__}")
        return None, None, None, STATUS_ERROR
    except Exception as e:
        logging.error(f"FILE {os.path.basename(filepath)} error={e}")
        return None, None, None, STATUS_ERROR

def get_openalex_exclusion_reason(message: Optional[Dict]) -> Optional[str]:
    if not message:
        return None
    clean_title = clean_ingest_text(message.get("title"))
    if not clean_title:
        return "missing-title"
    return classify_ingest_exclusion(
        clean_title,
        message.get("type"),
        source="openalex",
    )


def get_or_create_subject_hierarchical(cursor: mariadb.Cursor, subject_data: Optional[dict], parent_id: Optional[int] = None, source_vocabulary: str = 'OpenAlex', subject_type_param: str = 'Topic') -> Optional[int]:
    if not subject_data or not subject_data.get('id') or not subject_data.get('display_name'): return None
    openalex_id = subject_data['id']
    display_name = subject_data['display_name'].strip()[:255]
    term_key = normalize_term_key(display_name)[:255]
    subject_db_id = None

    if not term_key: return None

    try:
        cursor.execute("SELECT id, parent_id FROM subjects WHERE external_uri = ?", (openalex_id,))
        result = cursor.fetchone()
        if result:
            subject_db_id = result[0]
            if result[1] is None and parent_id is not None:
                try: cursor.execute("UPDATE subjects SET parent_id = ? WHERE id = ?", (parent_id, subject_db_id))
                except mariadb.Error: pass
            return subject_db_id
    except mariadb.Error: pass

    try:
        cursor.execute("SELECT id, parent_id, external_uri FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ?", (source_vocabulary, subject_type_param, term_key))
        result = cursor.fetchone()
        if result:
            subject_db_id = result[0]
            return subject_db_id
    except mariadb.Error: pass

    try:
        cursor.execute("""INSERT INTO subjects (term, vocabulary, subject_type, lang, external_uri, parent_id, term_key) VALUES (?, ?, ?, 'en', ?, ?, ?)""",
            (display_name, source_vocabulary, subject_type_param, openalex_id, parent_id, term_key))
        return cursor.lastrowid
    except mariadb.IntegrityError:
        cursor.execute("SELECT id FROM subjects WHERE external_uri = ?", (openalex_id,))
        if result := cursor.fetchone(): return result[0]
        return None
    except mariadb.Error: return None



def sync_publication_details(cursor: mariadb.Cursor, publication_id: int, message: Dict, doi: Optional[str], openalex_id: Optional[str], cache: Dict[str, Dict]) -> bool:
    try:
        cursor.execute("""SELECT venue_id, publisher_id, publication_date, volume, issue, pages, source, isbn, source_indexed_at, source_deposited_at, source_prefix, source_member_id, doi, pmid, pmcid, arxiv, wos_id, wikidata_id, openalex_id FROM publications WHERE id = ?""", (publication_id,))
        current_pub = cursor.fetchone()
        if not current_pub: return False

        pub_updates, pub_params = [], []
        primary_location = message.get('primary_location') or {}
        source_data = primary_location.get('source') or {}
        biblio_data = message.get('biblio') or {}
        ids_data = message.get('ids') or {}

        if not current_pub[0] and source_data:
            venue_name = clean_ingest_text(source_data.get('display_name'))
            issn_list = source_data.get('issn', [])
            print_issn = issn_list[0] if (issn_list and len(issn_list) > 0) else source_data.get('issn_l')
            e_issn = issn_list[1] if (issn_list and len(issn_list) > 1) else None
            oa_venue_type = source_data.get('type'); venue_type = VENUE_TYPE_MAP.get(oa_venue_type, VENUE_TYPE_OTHER)
            venue_openalex_id = source_data.get('id')
            if venue_id := get_or_create_venue(cursor, venue_name, print_issn, e_issn, venue_type, cache, openalex_id=venue_openalex_id):
                pub_updates.append("venue_id = ?"); pub_params.append(venue_id)
        
        if not current_pub[1] and source_data:
            publisher_name = clean_ingest_text(source_data.get('host_organization_name'))
            publisher_openalex_id = source_data.get('host_organization')
            if publisher_id := get_or_create_organization(
                cursor,
                publisher_name,
                ORG_TYPE_PUBLISHER,
                cache,
                openalex_id=publisher_openalex_id,
            ):
                pub_updates.append("publisher_id = ?"); pub_params.append(publisher_id)
        
        if not current_pub[2] and (pub_date := extract_publication_date(message)): pub_updates.append("publication_date = ?"); pub_params.append(pub_date)
        if not current_pub[3] and (volume := biblio_data.get('volume')): pub_updates.append("volume = ?"); pub_params.append(str(volume)[:50])
        if not current_pub[4] and (issue := biblio_data.get('issue')): pub_updates.append("issue = ?"); pub_params.append(str(issue)[:50])
        if not current_pub[5]:
            first_page, last_page = biblio_data.get('first_page'), biblio_data.get('last_page'); pages = None
            if first_page and last_page: pages = f"{first_page}-{last_page}"
            elif first_page: pages = str(first_page)
            if pages: pub_updates.append("pages = ?"); pub_params.append(str(pages)[:255])
        if not current_pub[7] and (isbn_list := ids_data.get('isbn', [])) and len(isbn_list) > 0:
            pub_updates.append("isbn = ?"); pub_params.append(str(isbn_list[0])[:20])

        if not current_pub[6]: pub_updates.append("source = ?"); pub_params.append("openalex")
        if not current_pub[8] and (idx_raw := message.get('updated_date')) and (ft := format_iso_timestamp(idx_raw)): pub_updates.append("source_indexed_at = ?"); pub_params.append(ft)
        if not current_pub[9] and (dep_raw := message.get('created_date')) and (ft := format_iso_timestamp(dep_raw)): pub_updates.append("source_deposited_at = ?"); pub_params.append(ft)
        if not current_pub[10] and doi and '/' in doi: pub_updates.append("source_prefix = ?"); pub_params.append(doi.split('/')[0][:50])
        if not current_pub[11] and source_data:
            member_id = source_data.get('host_organization')
            if member_id and 'openalex.org/' in member_id: member_id = member_id.split('/')[-1]
            if member_id: pub_updates.append("source_member_id = ?"); pub_params.append(member_id[:50])
        
        if doi and not current_pub[12]: pub_updates.append("doi = ?"); pub_params.append(doi[:255])
        
        current_pmid = current_pub[13]
        current_pmcid = current_pub[14]
        current_arxiv = current_pub[15]
        current_wos_id = current_pub[16]
        current_wikidata_id = current_pub[17]
        current_openalex_id = current_pub[18]
        
        json_pmid_raw = ids_data.get('pmid')
        json_pmid = None 
        if json_pmid_raw and isinstance(json_pmid_raw, str):
            
            if match := re.search(r'(\d{5,12})', json_pmid_raw):
                json_pmid = match.group(1)
            
            elif re.fullmatch(r'\d{5,12}', json_pmid_raw.strip()):
                json_pmid = json_pmid_raw.strip()

        json_pmcid_raw = ids_data.get('pmcid')
        json_pmcid = None 
        if json_pmcid_raw and isinstance(json_pmcid_raw, str):
            
            if match := re.search(r'(PMC\d+)', json_pmcid_raw, re.IGNORECASE): 
                json_pmcid = match.group(1).upper()
            
            elif re.fullmatch(r'\d{1,15}', json_pmcid_raw.strip()): 
                json_pmcid = 'PMC' + json_pmcid_raw.strip()
            
        
        json_arxiv = ids_data.get('arxiv')
        if json_arxiv and 'arxiv.org/' in json_arxiv: json_arxiv = json_arxiv.split('/')[-1]

        if json_pmid and not current_pmid: pub_updates.append("pmid = ?"); pub_params.append(json_pmid[:20])
        if json_pmcid and not current_pmcid: pub_updates.append("pmcid = ?"); pub_params.append(json_pmcid[:20])
        if json_arxiv and not current_arxiv: pub_updates.append("arxiv = ?"); pub_params.append(str(json_arxiv)[:30])
        if not current_wos_id and (json_wos := ids_data.get('wos') or ids_data.get('wos_id')):
            pub_updates.append("wos_id = ?"); pub_params.append(str(json_wos).strip()[:30])
        if not current_wikidata_id and (json_wikidata := normalize_wikidata_id(ids_data.get('wikidata'))):
            pub_updates.append("wikidata_id = ?"); pub_params.append(json_wikidata)
        if openalex_id and not current_openalex_id: pub_updates.append("openalex_id = ?"); pub_params.append(openalex_id[:50])

        if pub_updates: 
            pub_params.append(publication_id)
            cursor.execute(f"UPDATE publications SET {', '.join(pub_updates)} WHERE id = ?", tuple(pub_params))
            updated_cols = [u.split('=')[0].strip() for u in pub_updates]
            logging.info(f"PUB {publication_id}: updated publications columns: {', '.join(updated_cols)}")
            return True
        return False
    except mariadb.Error as e: 
        logging.error(f"PUB {publication_id} error=sql detail={e}")
        return False

def sync_authorships(cursor: mariadb.Cursor, work_id: int, authorships_from_json: List[Dict], cache: Dict[str, Dict], is_new: bool = False) -> bool:
    del is_new  # OpenAlex payload remains author-only; reconcile helper handles reruns safely.

    if not authorships_from_json:
        return False

    resolved_contributors = []
    duplicate_author_entries = 0
    processed_person_ids = set()
    for i, authorship_item in enumerate(authorships_from_json, 1):
        author_data = authorship_item.get('author') or {}
        if not author_data:
            continue

        person_id = get_or_create_person(cursor, author_data, cache)
        if not person_id:
            continue
        if person_id in processed_person_ids:
            duplicate_author_entries += 1
            continue
        processed_person_ids.add(person_id)

        affiliation_name = None
        affiliation_openalex_id = None
        affiliation_ror_id = None
        affiliation_country = None
        if aff_list := (authorship_item.get('institutions') or []):
            if isinstance(aff_list, list) and aff_list and isinstance(aff_list[0], dict):
                affiliation_name = aff_list[0].get('display_name')
                affiliation_openalex_id = aff_list[0].get('id')
                affiliation_ror_id = aff_list[0].get('ror')
                affiliation_country = aff_list[0].get('country_code')
        if not affiliation_openalex_id:
            if affiliations := authorship_item.get('affiliations'):
                if isinstance(affiliations, list) and affiliations:
                    institution_ids = affiliations[0].get('institution_ids') if isinstance(affiliations[0], dict) else None
                    if institution_ids and isinstance(institution_ids, list) and institution_ids:
                        affiliation_openalex_id = institution_ids[0]
        affiliation_id = get_or_create_organization(
            cursor,
            affiliation_name,
            ORG_TYPE_INSTITUTE,
            cache,
            ror_id=affiliation_ror_id,
            openalex_id=affiliation_openalex_id,
            country_code=affiliation_country,
        )

        resolved_contributors.append(
            {
                "person_id": person_id,
                "role": "AUTHOR",
                "position": i,
                "affiliation_id": affiliation_id,
                "is_corresponding": 1 if authorship_item.get('is_corresponding') else 0,
                "display_name": author_data.get("display_name"),
            }
        )

    if not resolved_contributors:
        return False

    try:
        result = reconcile_resolved_authorships(
            cursor,
            work_id,
            resolved_contributors,
            allow_reclassification=False,
        )
    except mariadb.Error as e:
        logging.error(f"WORK {work_id} error=authorships-reconcile detail={e}")
        return False

    if result["changed"]:
        logging.info(
            f"WORK {work_id}: authorships inserted={result['inserted']} "
            f"updated={result['updated']} reclassified={result['reclassified']}"
        )
    if duplicate_author_entries:
        logging.debug(f"WORK {work_id}: skipped {duplicate_author_entries} duplicate author entries (same person_id)")
    return result["changed"]

def sync_funding(cursor: mariadb.Cursor, work_id: int, funders_from_json: List[Dict], cache: Dict[str, Dict], is_new: bool = False) -> bool:
    if not funders_from_json: return False
    if is_new:
        existing_funding = set()
    else:
        try:
            cursor.execute("SELECT funder_id, grant_number FROM funding WHERE work_id = ?", (work_id,))
            existing_funding = {(row[0], row[1]) for row in cursor.fetchall()}
        except mariadb.Error: return False
    funders_to_add = []
    
    for grant_data in funders_from_json: 
        funder_openalex_id = grant_data.get('funder')
        funder_ror_id = grant_data.get('funder_ror') or grant_data.get('ror')
        funder_id = get_or_create_organization(
            cursor,
            grant_data.get('funder_display_name'),
            ORG_TYPE_FUNDER,
            cache,
            ror_id=funder_ror_id,
            openalex_id=funder_openalex_id,
        )
        if not funder_id: continue
        grant_number = grant_data.get('award_id')
        if grant_number: grant_number = str(grant_number).strip()[:100]
        if (funder_id, grant_number) not in existing_funding: 
            funders_to_add.append((work_id, funder_id, grant_number)); existing_funding.add((funder_id, grant_number))
            
    if funders_to_add:
        try:
            cursor.executemany("INSERT IGNORE INTO funding (work_id, funder_id, grant_number) VALUES (?, ?, ?)", funders_to_add)
            if cursor.rowcount > 0:
                logging.info(f"WORK {work_id}: inserted {cursor.rowcount}/{len(funders_to_add)} funding rows")
            return cursor.rowcount > 0
        except mariadb.Error as e:
            logging.error(f"WORK {work_id} error=funding-insert detail={e}")
    return False

def sync_licenses(cursor: mariadb.Cursor, publication_id: int, location_data: Optional[Dict]) -> bool:
    if not location_data: location_data = {}
    license_url = location_data.get('license_id')
    content_version = location_data.get('version')
    license_value = ""
    if license_url:
        license_value = str(license_url).strip()
        if "openalex.org/licenses/" in license_value:
            license_value = license_value.split("openalex.org/licenses/")[-1]
        if "/" in license_value:
            license_value = license_value.split("/")[-1]
        license_value = license_value.strip().lower()
    if content_version:
        content_version = str(content_version).strip()
    if not license_value and not content_version:
        return False
    try:
        cursor.execute("SELECT license_url, license_version FROM publications WHERE id = ?", (publication_id,))
        row = cursor.fetchone()
    except mariadb.Error:
        return False
    if not row:
        return False
    current_url, current_version = row

    incoming_url = license_value[:512] if license_value else None
    incoming_version = content_version[:50] if content_version else None

    new_url = current_url
    if incoming_url and not current_url:
        new_url = incoming_url
    elif incoming_url and current_url:
        normalized_current_url = str(current_url).strip().lower()
        if "openalex.org/licenses/" in normalized_current_url:
            normalized_current_url = normalized_current_url.split("openalex.org/licenses/")[-1]
        if "/" in normalized_current_url:
            normalized_current_url = normalized_current_url.split("/")[-1]
        if normalized_current_url == incoming_url:
            new_url = incoming_url

    new_version = current_version
    if incoming_version and not current_version:
        new_version = incoming_version
    elif incoming_version and current_version:
        current_rank = LICENSE_VERSION_RANK.get(str(current_version).strip().lower(), 0)
        incoming_rank = LICENSE_VERSION_RANK.get(str(incoming_version).strip().lower(), 0)
        if incoming_rank >= current_rank:
            new_version = incoming_version

    if new_url == current_url and new_version == current_version:
        return False
    cursor.execute(
        "UPDATE publications SET license_url = ?, license_version = ? WHERE id = ?",
        (new_url, new_version, publication_id),
    )
    if cursor.rowcount > 0:
        logging.info(f"PUBLICATION {publication_id}: updated license")
    return cursor.rowcount > 0

def sync_topics_hierarchical(cursor: mariadb.Cursor, work_id: int, topics_list: list):
    if not topics_list: return False
    associated_ids = set(); changes_made = False
    
    for topic_info in topics_list:
        try:
            domain_id = get_or_create_subject_hierarchical(cursor, topic_info.get('domain'), None, 'OpenAlex', 'Domain')
            field_id = get_or_create_subject_hierarchical(cursor, topic_info.get('field'), domain_id, 'OpenAlex', 'Field')
            subfield_id = get_or_create_subject_hierarchical(cursor, topic_info.get('subfield'), field_id, 'OpenAlex', 'Subfield')
            topic_id = get_or_create_subject_hierarchical(cursor, topic_info, subfield_id, 'OpenAlex', 'Topic')
            if topic_id: associated_ids.add(topic_id)
        except Exception: pass

    if not associated_ids: return False

    placeholders = ', '.join(['(?, ?)'] * len(associated_ids));
    params = [val for tid in list(associated_ids) for val in (work_id, tid)]
    try:
        cursor.execute(f"INSERT IGNORE INTO work_subjects (work_id, subject_id) VALUES {placeholders}", tuple(params))
        changes_made = cursor.rowcount > 0
        if changes_made:
            logging.info(f"WORK {work_id}: linked {cursor.rowcount}/{len(associated_ids)} topics")
        return changes_made
    except mariadb.Error: return False

def sync_keywords(cursor: mariadb.Cursor, work_id: int, keywords_list: List[Dict], cache: Dict[str, Dict], is_new: bool = False) -> bool:
    if not keywords_list: return False
    terms_from_json = [kw.get('display_name') for kw in keywords_list if kw.get('display_name')]
    if not terms_from_json: return False

    if is_new:
        existing_term_keys = set()
    else:
        try:
            cursor.execute("SELECT s.term_key FROM work_subjects ws JOIN subjects s ON ws.subject_id = s.id WHERE ws.work_id = ? AND s.vocabulary = 'KEYWORD'", (work_id,))
            existing_term_keys = {row[0] for row in cursor.fetchall()}
        except mariadb.Error: return False

    subjects_to_add = []
    processed_term_keys_in_batch = set()

    for term in terms_from_json:
        if not term or not isinstance(term, str): continue
        term_key = normalize_term_key(term.strip().lower()[:255])
        if not term_key or term_key in existing_term_keys or term_key in processed_term_keys_in_batch: continue
        
        subject_id = get_or_create_subject(cursor, term, cache)
        if subject_id: 
            subjects_to_add.append((work_id, subject_id))
            existing_term_keys.add(term_key)
            processed_term_keys_in_batch.add(term_key)
    
    if subjects_to_add:
        cursor.executemany("INSERT IGNORE INTO work_subjects (work_id, subject_id) VALUES (?, ?)", subjects_to_add)
        if cursor.rowcount > 0:
            logging.info(f"WORK {work_id}: linked {cursor.rowcount}/{len(subjects_to_add)} keywords")
            return True
    return False

def sync_citations(cursor: mariadb.Cursor, work_id: int, referenced_works: List[str], is_new: bool = False) -> bool:
    if not referenced_works: return False
    unique_cited_openalex_ids = {ref.split('/')[-1] for ref in referenced_works if ref and 'openalex.org/' in ref}
    if not unique_cited_openalex_ids: return False
    try:
        resolved_dois = set()
        batch_size = 1000; ids_list = list(unique_cited_openalex_ids)
        for i in range(0, len(ids_list), batch_size):
            batch_ids = ids_list[i:i + batch_size]; placeholders = ', '.join(['?'] * len(batch_ids))
            cursor.execute(
                f"SELECT p.doi FROM publications p WHERE p.openalex_id IN ({placeholders}) AND p.doi IS NOT NULL",
                tuple(batch_ids),
            )
            for row in cursor.fetchall():
                norm_doi = normalize_doi(row[0])
                if norm_doi:
                    resolved_dois.add(norm_doi)

        if not resolved_dois:
            return False

        if is_new:
            existing_dois = set()
        else:
            existing_dois = set()
            dois_list = list(resolved_dois)
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

        refs_to_add = [(work_id, doi[:255]) for doi in resolved_dois if doi not in existing_dois]
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

def sync_best_oa_file(cursor: mariadb.Cursor, publication_id: int, message: Dict) -> bool:
           
    best_oa = message.get('best_oa_location')
    if not best_oa or not isinstance(best_oa, dict):
        return False

    pdf_url = best_oa.get('pdf_url')
    landing_url = best_oa.get('landing_page_url')
    
    oa_location_id = best_oa.get('id')
    if oa_location_id:
         oa_location_id = str(oa_location_id)[:255] 
    
    target_url = pdf_url if pdf_url else landing_url
    
    if not target_url:
        return False 

    file_format = 'PDF' if pdf_url else 'HTML'
    
    url_hash = hashlib.md5(target_url.strip().encode('utf-8')).hexdigest()
    
    try:
        cursor.execute("""
            INSERT INTO files
            (md5, publication_id, best_oa_url, openacess_id, file_format, verification_status)
            VALUES (?, ?, ?, ?, ?, 'PENDING')
            ON DUPLICATE KEY UPDATE
                best_oa_url = COALESCE(VALUES(best_oa_url), best_oa_url),
                openacess_id = COALESCE(VALUES(openacess_id), openacess_id),
                file_format = COALESCE(VALUES(file_format), file_format)
        """, (url_hash, publication_id, target_url, oa_location_id, file_format))
        file_changed = cursor.rowcount > 0

        cursor.execute("SELECT id FROM files WHERE md5 = ? AND publication_id = ?", (url_hash, publication_id))
        result = cursor.fetchone()
        
        if not result:
            logging.error(f"PUB {publication_id} error=file-id-not-found url={target_url}")
            return False
        
        file_id = result[0]
        if file_changed:
            logging.info(f"PUB {publication_id}: linked file_id={file_id} format={file_format}")
        return file_changed

    except mariadb.Error as e:
        logging.error(f"PUB {publication_id} error=best-oa-file detail={e}")
        return False


def _process_record_no_tx(
    cursor: mariadb.Cursor,
    filepath: str,
    message: Optional[Dict],
    openalex_id: Optional[str],
    doi: Optional[str],
    mode: str,
    cache: Dict[str, Dict],
) -> str:
    work_id: Optional[int] = None
    publication_id: Optional[int] = None

    if message:
        clean_title = clean_ingest_text(message.get("title"))
        if not clean_title:
            return STATUS_SKIPPED

    if mode != "fast" and doi:
        cursor.execute("SELECT p.work_id, p.id FROM publications p WHERE p.doi = ?", (doi,))
        if result := cursor.fetchone():
            if mode == "new":
                return STATUS_SKIPPED
            work_id, publication_id = result

    # Fallback identifiers are only used when DOI is absent:
    # DOI is the primary match key; openalex_id is a fallback key.
    if not work_id and not doi and openalex_id:
        cursor.execute("SELECT p.work_id, p.id FROM publications p WHERE p.openalex_id = ?", (openalex_id,))
        if result_pub := cursor.fetchone():
            work_id, publication_id = result_pub

    if mode == "update" and not work_id:
        return STATUS_SKIPPED

    if not message:
        return STATUS_SKIPPED

    changes_made = False

    if work_id and publication_id:
        cursor.execute("SELECT title, work_type, abstract, language FROM works WHERE id = ?", (work_id,))
        current_work = cursor.fetchone()
        if not current_work:
            raise mariadb.DatabaseError(f"Work ID {work_id} desapareceu.")

        current_title, current_work_type, current_abstract, current_language = current_work
        json_title = clean_ingest_text(message.get('title'))
        if json_title:
            json_title = json_title[:65535]
        json_work_type = WORK_TYPE_MAP.get(message.get('type', 'other').lower(), 'OTHER')
        json_abstract = reconstruct_abstract(message.get('abstract_inverted_index'))
        json_language = normalize_language_code(message.get('language'))

        work_updates, work_params = [], []
        if (not current_title or current_title == 'Title Unavailable') and json_title:
            work_updates.append("title = ?")
            work_params.append(json_title)
        if (not current_work_type or current_work_type == 'OTHER') and (json_work_type and json_work_type != 'OTHER'):
            work_updates.append("work_type = ?")
            work_params.append(json_work_type)
        if json_abstract and not current_abstract:
            work_updates.append("abstract = ?")
            work_params.append(json_abstract[:16777215])
        if json_language and not current_language:
            work_updates.append("language = ?")
            work_params.append(json_language)

        if work_updates:
            work_params.append(work_id)
            cursor.execute(f"UPDATE works SET {', '.join(work_updates)} WHERE id = ?", tuple(work_params))
            changes_made = changes_made or cursor.rowcount > 0
            if cursor.rowcount > 0:
                updated_cols = [u.split('=')[0].strip() for u in work_updates]
                logging.info(f"WORK {work_id} updated={', '.join(updated_cols)}")

        changes = [changes_made]
        changes.append(sync_publication_details(cursor, publication_id, message, doi, openalex_id, cache))
        changes.append(sync_authorships(cursor, work_id, message.get('authorships') or [], cache))
        changes.append(sync_citations(cursor, work_id, message.get('referenced_works') or []))
        changes.append(sync_funding(cursor, work_id, message.get('grants') or [], cache))
        changes.append(sync_licenses(cursor, publication_id, message.get('primary_location')))
        changes.append(sync_topics_hierarchical(cursor, work_id, message.get('topics') or []))
        changes.append(sync_keywords(cursor, work_id, message.get('keywords') or [], cache))
        changes.append(sync_best_oa_file(cursor, publication_id, message))

        if mode == "update":
            expected_authors = sum(
                1 for entry in (message.get('authorships') or []) if isinstance(entry, dict)
            )
            confirm_author_count(cursor, work_id, expected_authors, "openalex")

        status = STATUS_UPDATED if any(changes) else STATUS_NO_CHANGE
        if any(changes):
            logging.info(
                f"FILE {os.path.basename(filepath)} status={status} doi={doi or '-'} openalex_id={openalex_id or '-'} work_id={work_id} publication_id={publication_id}"
            )
        return status

    title = clean_ingest_text(message.get('title'))
    if not title:
        return STATUS_SKIPPED
    title = title[:65535]
    work_type_enum = WORK_TYPE_MAP.get(message.get('type', 'other').lower(), 'OTHER')
    abstract = reconstruct_abstract(message.get('abstract_inverted_index'))

    language = normalize_language_code(message.get('language'))
    cursor.execute("INSERT INTO works (title, work_type, abstract, language) VALUES (?, ?, ?, ?)",
                   (title, work_type_enum, abstract[:16777215] if abstract else None, language))
    work_id = cursor.lastrowid

    safe_doi = doi[:255] if doi else None
    safe_oai = openalex_id[:50] if openalex_id else None
    cursor.execute("INSERT INTO publications (work_id, doi, openalex_id) VALUES (?, ?, ?)",
                   (work_id, safe_doi, safe_oai))
    publication_id = cursor.lastrowid

    sync_publication_details(cursor, publication_id, message, doi, openalex_id, cache)
    sync_authorships(cursor, work_id, message.get('authorships') or [], cache, is_new=True)
    sync_citations(cursor, work_id, message.get('referenced_works') or [], is_new=True)
    sync_funding(cursor, work_id, message.get('grants') or [], cache, is_new=True)
    sync_licenses(cursor, publication_id, message.get('primary_location'))
    sync_topics_hierarchical(cursor, work_id, message.get('topics') or [])
    sync_keywords(cursor, work_id, message.get('keywords') or [], cache, is_new=True)
    sync_best_oa_file(cursor, publication_id, message)

    logging.info(
        f"FILE {os.path.basename(filepath)} status={STATUS_INSERTED} doi={doi or '-'} openalex_id={openalex_id or '-'} work_id={work_id} publication_id={publication_id}"
    )
    return STATUS_INSERTED

def _openalex_parse_adapter(filepath):
    """Adapter: runner expects (message, doi, parse_status); parse_openalex_file returns 4 values.
    We stash openalex_id in message['_openalex_id'] for the process adapter."""
    message, openalex_id, doi, parse_status = parse_openalex_file(filepath)
    if message is not None and openalex_id:
        message['_openalex_id'] = openalex_id
    return message, doi, parse_status


def _openalex_process_no_tx_adapter(cursor, filepath, message, doi, mode, cache):
    """Adapter: extracts openalex_id from message and delegates."""
    openalex_id = message.pop('_openalex_id', None) if message else None
    return _process_record_no_tx(cursor, filepath, message, openalex_id, doi, mode, cache)


def _openalex_process_per_file(conn, filepath, message, doi, mode, cache):
    """Per-file transaction wrapper for runner's per-file mode."""
    cursor = None
    try:
        cursor = conn.cursor()
        conn.begin()
        openalex_id = message.pop('_openalex_id', None) if message else None
        status = _process_record_no_tx(cursor, filepath, message, openalex_id, doi, mode, cache)
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
        detail = str(e) or type(e).__name__
        logging.error(f"FILE {os.path.basename(filepath)} error={detail}")
        return STATUS_ERROR
    finally:
        if cursor:
            try:
                cursor.close()
            except mariadb.Error:
                pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Ingest records from OpenAlex JSON files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_work_loader_arguments(parser)
    args = parser.parse_args()
    run_work_loader(
        args,
        parse_file=_openalex_parse_adapter,
        get_exclusion_reason=get_openalex_exclusion_reason,
        process_record_no_tx=_openalex_process_no_tx_adapter,
        process_record_per_file=_openalex_process_per_file,
    )
