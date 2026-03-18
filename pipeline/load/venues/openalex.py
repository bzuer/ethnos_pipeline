"""
populate_venues_openalex.py

Script para unificar, verificar e enriquecer venues a partir de arquivos JSON
do OpenAlex.

Regras de matching de venue:
- Prioridade: ISSN/eISSN -> IDs (openalex/wikidata/mag) -> nome + tipo.

Recuperação de conflitos de unicidade em INSERT:
- openalex_id, wikidata_id, mag_id, issn/eissn, name+type.
"""
import os
import sys
import json
import mariadb
import logging
import argparse
import re
import csv
from typing import Dict, Any, Optional, List, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INGEST_DIR = os.path.dirname(SCRIPT_DIR)
if INGEST_DIR not in sys.path:
    sys.path.insert(0, INGEST_DIR)

from common import get_connection


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("enrich_venues.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)





VENUE_TYPE_OTHER = 'OTHER'


VENUE_TYPE_MAP = {
    'journal': 'JOURNAL',
    'tradejournal': 'JOURNAL',
    'conference': 'CONFERENCE',
    'conferenceproceeding': 'CONFERENCE',
    'repository': 'REPOSITORY',
    'book-series': 'BOOK_SERIES',
    'bookseries': 'BOOK_SERIES',
    'ebook-platform': 'OTHER',
    'other': 'OTHER'
}

VENUE_MERGE_UNIQUE_FIELDS = ("issn", "eissn", "scopus_id", "wikidata_id", "openalex_id", "mag_id")
VENUE_MERGE_FILL_IF_NULL_FIELDS = (
    "abbreviated_name",
    "publisher_id",
    "country_code",
    "issn",
    "eissn",
    "homepage_url",
    "aggregation_type",
    "scopus_id",
    "wikidata_id",
    "openalex_id",
    "scielo_id",
    "mag_id",
)
VENUE_MERGE_BOOL_MAX_FIELDS = ("open_access", "is_in_doaj", "is_in_scielo", "is_indexed_in_scopus")


def get_repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def normalize_venue_type(raw_type: Optional[str]) -> str:
    if not raw_type:
        return VENUE_TYPE_OTHER
    return VENUE_TYPE_MAP.get(str(raw_type).strip().lower(), VENUE_TYPE_OTHER)


def extract_identifier(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    raw = str(raw_value).strip()
    if not raw:
        return None
    if '/' in raw:
        raw = raw.rstrip('/').split('/')[-1]
    return raw.strip() or None


def normalize_issn(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    raw = str(raw_value).strip().upper().replace("-", "")
    if not raw:
        return None
    if re.fullmatch(r"[0-9X]{8}", raw):
        return f"{raw[:4]}-{raw[4:]}"
    if re.fullmatch(r"[0-9X]{4}-[0-9X]{4}", str(raw_value).strip().upper()):
        return str(raw_value).strip().upper()
    return None


def row_to_dict(cursor: mariadb.Cursor, row: Optional[tuple]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    colnames = [d[0] for d in cursor.description]
    return dict(zip(colnames, row))


def get_or_create_publisher(cursor: mariadb.Cursor, publisher_name: str) -> Optional[int]:
                                                             
    if not publisher_name or not (name_stripped := publisher_name.strip()): return None
    name_lower = name_stripped.lower()
    publisher_id = None
    try:
        cursor.execute("SELECT id FROM organizations WHERE standardized_name = ?", (name_lower,))
        if result := cursor.fetchone(): publisher_id = result[0]
    except mariadb.Error as e_std: log.error(f"Erro busca publisher '{name_stripped}': {e_std}"); return None

    if publisher_id: return publisher_id

    try:
        cursor.execute("INSERT INTO organizations (name, type) VALUES (?, 'PUBLISHER')", (name_stripped,))
        new_id = cursor.lastrowid
        log.debug(f"    -> Criado Publisher: '{name_stripped}', ID: {new_id}")
        return new_id
    except mariadb.IntegrityError:
        log.warning(f"Conflito ao inserir publisher '{name_stripped}'. Buscando novamente.")
        try:
            cursor.execute("SELECT id FROM organizations WHERE standardized_name = ?", (name_lower,))
            if result := cursor.fetchone(): return result[0]
            return None
        except mariadb.Error as e_retry: log.error(f"Erro busca publisher '{name_stripped}' pós-conflito: {e_retry}"); return None
    except mariadb.Error as e_ins: log.error(f"Erro DB insert publisher '{name_stripped}': {e_ins}"); return None

def normalize_term_key(term: str) -> str:
                                                                    
    if not term: return ""
    cleaned = re.sub(r'[-_/,:;.\'"`()\[\]"\'&|]+', ' ', term.lower())
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def get_or_create_subject_hierarchical(
    cursor: mariadb.Cursor,
    subject_data: Optional[dict],
    parent_id: Optional[int] = None,
    source_vocabulary: str = 'OpenAlex',
    subject_type_param: str = 'Topic'
) -> Optional[int]:
                                                                                          
    if not subject_data or not subject_data.get('id') or not subject_data.get('display_name'):
        return None

    openalex_id = subject_data['id']
    display_name = subject_data['display_name'].strip()
    term_key = normalize_term_key(display_name)
    subject_db_id = None
    existing_parent_id = None

    if not term_key:
        log.warning(f"Termo '{display_name}' ({openalex_id}) resultou em term_key vazio. Pulando.")
        return None

    try:
        cursor.execute("SELECT id, parent_id FROM subjects WHERE external_uri = ?", (openalex_id,))
        result = cursor.fetchone()
        if result:
            subject_db_id, existing_parent_id = result[0], result[1]
            log.debug(f"    -> Encontrado Subject por URI: '{display_name}' ID:{subject_db_id}")
            if existing_parent_id is None and parent_id is not None:
                try:
                    cursor.execute("UPDATE subjects SET parent_id = ? WHERE id = ?", (parent_id, subject_db_id))
                    log.debug(f"    -> Parent ID atualizado para {parent_id} em Subject ID:{subject_db_id}")
                except mariadb.Error as e_upd_p:
                    log.error(f"    -> Erro ao atualizar Parent ID {subject_db_id} (via URI find): {e_upd_p}")
            return subject_db_id
    except mariadb.Error as e_sel_uri:
        log.error(f"Erro busca subject por URI '{openalex_id}': {e_sel_uri}")

    try: 
        cursor.execute("SELECT id, parent_id, external_uri FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ?",
                       (source_vocabulary, subject_type_param, term_key))
        result = cursor.fetchone()
        if result:
            subject_db_id, existing_parent_id, existing_uri = result[0], result[1], result[2]
            log.debug(f"    -> Encontrado Subject por Chave Única: '{display_name}' ID:{subject_db_id}")
            updates_needed = []
            params = []
            if existing_uri is None and openalex_id is not None:
                updates_needed.append("external_uri = ?")
                params.append(openalex_id)
            if existing_parent_id is None and parent_id is not None:
                updates_needed.append("parent_id = ?")
                params.append(parent_id)

            if updates_needed:
                params.append(subject_db_id)
                try:
                    cursor.execute(f"UPDATE subjects SET {', '.join(updates_needed)} WHERE id = ?", tuple(params))
                    log.debug(f"    -> Dados faltantes atualizados em Subject ID:{subject_db_id} (via Key find)")
                except mariadb.Error as e_upd_k:
                    log.error(f"    -> Erro ao atualizar dados faltantes {subject_db_id} (via Key find): {e_upd_k}")
            return subject_db_id
    except mariadb.Error as e_sel_key:
        log.error(f"Erro busca subject por Chave Única '{term_key}': {e_sel_key}")

    try:
        log.debug(f"    -> Tentando inserir Subject: '{display_name}' ({source_vocabulary}/{subject_type_param}) Parent:{parent_id} URI:{openalex_id} Key:'{term_key}'")
        cursor.execute(
            """INSERT INTO subjects
               (term, vocabulary, subject_type, lang, external_uri, parent_id, term_key)
               VALUES (?, ?, ?, 'en', ?, ?, ?)""",
            (display_name, source_vocabulary, subject_type_param, openalex_id, parent_id, term_key)
        )
        subject_db_id = cursor.lastrowid
        log.debug(f"    -> Criado Subject: '{display_name}' ({source_vocabulary}/{subject_type_param}), ID:{subject_db_id}, Parent:{parent_id}")
        return subject_db_id
    except mariadb.IntegrityError as e_integrity:
        log.warning(f"    -> Conflito ao inserir Subject '{display_name}' ({openalex_id}). Tentando buscar novamente...")
        try:
            cursor.execute("SELECT id FROM subjects WHERE external_uri = ?", (openalex_id,))
            result = cursor.fetchone()
            if result:
                log.debug(f"    -> Encontrado Subject ID:{result[0]} por URI após conflito.")
                return result[0]
            else:
                cursor.execute("SELECT id FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ?",
                               (source_vocabulary, subject_type_param, term_key))
                result = cursor.fetchone()
                if result:
                    log.debug(f"    -> Encontrado Subject ID:{result[0]} por Chave Única após conflito.")
                    try: cursor.execute("UPDATE subjects SET external_uri = COALESCE(external_uri, ?) WHERE id = ?", (openalex_id, result[0]))
                    except mariadb.Error: pass
                    return result[0]
                else:
                    log.error(f"CRITICAL: Falha ao inserir E falha ao re-buscar Subject '{display_name}' ({openalex_id}). Abortando para este subject.")
                    return None
        except mariadb.Error as e_retry:
            log.error(f"Erro DB ao re-buscar Subject '{display_name}' ({openalex_id}) após conflito: {e_retry}")
            return None
    except mariadb.Error as e_ins:
        log.error(f"Erro DB inesperado ao inserir Subject '{display_name}' ({openalex_id}): {e_ins}")
        return None
    except Exception as e_gen:
        log.error(f"Erro Python inesperado em get_or_create_subject '{display_name}' ({openalex_id}): {e_gen}", exc_info=True)
        return None


def update_venue_subjects_hierarchical(cursor: mariadb.Cursor, venue_id: int, topics_list: list) -> int:
    """Process hierarchical subjects for a venue. Returns number of new associations."""
    if not topics_list: return 0
    associated_ids = set(); has_errors = False
    for topic_info in topics_list:
        try:
            domain_d, field_d, subfield_d = topic_info.get('domain'), topic_info.get('field'), topic_info.get('subfield')
            domain_id = get_or_create_subject_hierarchical(cursor, domain_d, None, 'OpenAlex', 'Domain')
            field_id = get_or_create_subject_hierarchical(cursor, field_d, domain_id, 'OpenAlex', 'Field')
            subfield_id = get_or_create_subject_hierarchical(cursor, subfield_d, field_id, 'OpenAlex', 'Subfield')
            topic_id = get_or_create_subject_hierarchical(cursor, topic_info, subfield_id, 'OpenAlex', 'Topic')

            if topic_id is None and topic_info.get('id') and topic_info.get('display_name'):
                log.error(f"Falha obter/criar ID tópico: {topic_info.get('display_name')} ({topic_info.get('id')}) para Venue {venue_id}", exc_info=False)
                has_errors = True
                continue

            if topic_id: associated_ids.add(topic_id)

        except Exception as e:
            log.error(f"Erro processar hierarquia tópico {topic_info.get('id', 'N/A')} para Venue {venue_id}: {e}", exc_info=True)
            has_errors = True

    if not associated_ids:
        return 0

    placeholders = ', '.join(['(?, ?)'] * len(associated_ids));
    params = [val for tid in list(associated_ids) for val in (venue_id, tid)]
    try:
        cursor.execute(f"INSERT IGNORE INTO venue_subjects (venue_id, subject_id) VALUES {placeholders}", tuple(params))
        return cursor.rowcount
    except mariadb.Error as e:
        log.error(f"    -> Erro associar TÓPICOS Venue {venue_id}: {e}")
        return 0


def collect_json_files(json_dir: str) -> List[str]:
    log.info(f"Mapeando JSONs em '{json_dir}'...")
    files: List[str] = []
    with_issn = 0
    without_issn = 0
    skipped = 0

    try:
        for filename in sorted(os.listdir(json_dir)):
            if not filename.lower().endswith('.json'):
                continue
            filepath = os.path.join(json_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                issn_l = data.get('issn_l')
                issn_list = data.get('issn') or []
                has_issn = bool(issn_l or any(issn_list))
                if has_issn:
                    with_issn += 1
                else:
                    without_issn += 1
                files.append(filepath)
            except (json.JSONDecodeError, IOError) as e:
                log.warning(f"Erro processar mapa {filename}: {e}")
                skipped += 1
            except Exception as e:
                log.error(f"Erro inesperado mapear {filename}: {e}", exc_info=True)
                skipped += 1
    except FileNotFoundError:
        log.error(f"Diretório JSON '{json_dir}' não encontrado.")
        return []
    except Exception as e:
        log.error(f"Erro ao listar diretório '{json_dir}': {e}", exc_info=True)
        return []

    log.info(
        "Mapeamento concluído. %s arquivos JSON válidos (%s com ISSN, %s sem ISSN). %s pulados.",
        len(files),
        with_issn,
        without_issn,
        skipped,
    )
    return files


def find_existing_venue(cursor: mariadb.Cursor, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    j_issn_l = data.get('issn_l')
    j_list = data.get('issn') or []
    search_issns = sorted({normalize_issn(issn) for issn in [j_issn_l] + j_list if normalize_issn(issn)})
    search_issns_compact = sorted({issn.replace("-", "") for issn in search_issns})
    ids = data.get('ids') or {}
    openalex_id = extract_identifier(ids.get('openalex'))
    wikidata_id = extract_identifier(ids.get('wikidata'))
    mag_id = extract_identifier(ids.get('mag'))
    venue_name = (data.get('display_name') or '').strip()
    venue_type = normalize_venue_type(data.get('type'))

    cols = (
        "id, name, type, issn, eissn, publisher_id, homepage_url, country_code, "
        "open_access, is_in_doaj, aggregation_type, scopus_id, openalex_id, wikidata_id, mag_id, "
        "is_indexed_in_scopus, cited_by_count, h_index, i10_index, `2yr_mean_citedness`"
    )
    sql_base = f"SELECT {cols} FROM venues"

    if search_issns and search_issns_compact:
        try:
            issn_ph = ','.join(['?'] * len(search_issns))
            compact_ph = ','.join(['?'] * len(search_issns_compact))
            params = tuple(search_issns) * 2 + tuple(search_issns_compact) * 2
            cursor.execute(
                (
                    f"{sql_base} WHERE ("
                    f"issn IN ({issn_ph}) OR eissn IN ({issn_ph}) "
                    f"OR REPLACE(issn, '-', '') IN ({compact_ph}) "
                    f"OR REPLACE(eissn, '-', '') IN ({compact_ph})"
                    f") LIMIT 1"
                ),
                params,
            )
            found = row_to_dict(cursor, cursor.fetchone())
            if found:
                return found
        except mariadb.Error as e:
            log.error(f"Erro ao buscar venue por ISSN: {e}")

    id_lookups: List[Tuple[str, str, Tuple[Any, ...]]] = []
    if openalex_id:
        id_lookups.append(("openalex_id", f"{sql_base} WHERE openalex_id = ? LIMIT 1", (openalex_id,)))
    if wikidata_id:
        id_lookups.append(("wikidata_id", f"{sql_base} WHERE wikidata_id = ? LIMIT 1", (wikidata_id,)))
    if mag_id:
        id_lookups.append(("mag_id", f"{sql_base} WHERE mag_id = ? LIMIT 1", (mag_id,)))

    for key_name, query, params in id_lookups:
        try:
            cursor.execute(query, params)
            found = row_to_dict(cursor, cursor.fetchone())
            if found:
                return found
        except mariadb.Error as e:
            log.error(f"Erro ao buscar venue por {key_name}: {e}")

    if venue_name:
        try:
            cursor.execute(
                f"{sql_base} WHERE name = ? AND type = ? LIMIT 1",
                (venue_name, venue_type),
            )
            found = row_to_dict(cursor, cursor.fetchone())
            if found:
                return found
        except mariadb.Error as e:
            log.error(f"Erro ao buscar venue por nome/tipo: {e}")

    return None


def recover_venue_id_after_conflict(cursor: mariadb.Cursor, new_venue: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    lookups: List[Tuple[str, str, Tuple[Any, ...]]] = []

    if new_venue.get('issn'):
        lookups.append(("issn", "SELECT id FROM venues WHERE issn = ? OR eissn = ? LIMIT 1", (new_venue['issn'], new_venue['issn'])))
    if new_venue.get('eissn'):
        lookups.append(("eissn", "SELECT id FROM venues WHERE issn = ? OR eissn = ? LIMIT 1", (new_venue['eissn'], new_venue['eissn'])))
    if new_venue.get('openalex_id'):
        lookups.append(("openalex_id", "SELECT id FROM venues WHERE openalex_id = ? LIMIT 1", (new_venue['openalex_id'],)))
    if new_venue.get('wikidata_id'):
        lookups.append(("wikidata_id", "SELECT id FROM venues WHERE wikidata_id = ? LIMIT 1", (new_venue['wikidata_id'],)))
    if new_venue.get('mag_id'):
        lookups.append(("mag_id", "SELECT id FROM venues WHERE mag_id = ? LIMIT 1", (new_venue['mag_id'],)))
    if new_venue.get('name') and new_venue.get('type'):
        lookups.append(("name+type", "SELECT id FROM venues WHERE name = ? AND type = ? LIMIT 1", (new_venue['name'], new_venue['type'])))

    for key_name, query, params in lookups:
        cursor.execute(query, params)
        result = cursor.fetchone()
        if result:
            return result[0], key_name
    return None, None


def find_conflict_id_by_unique_field(
    cursor: mariadb.Cursor,
    field: str,
    value: Any,
    current_venue_id: int,
) -> Optional[int]:
    if value is None:
        return None
    cursor.execute(f"SELECT id FROM venues WHERE {field} = ? LIMIT 1", (value,))
    result = cursor.fetchone()
    if result and result[0] != current_venue_id:
        return result[0]
    return None


def has_name_type_conflict(
    cursor: mariadb.Cursor,
    name: Optional[str],
    venue_type: Optional[str],
    current_venue_id: int,
) -> Optional[int]:
    if not name or not venue_type:
        return None
    cursor.execute(
        "SELECT id FROM venues WHERE name = ? AND type = ? LIMIT 1",
        (name, venue_type),
    )
    result = cursor.fetchone()
    if result and result[0] != current_venue_id:
        return result[0]
    return None


def _get_venue_for_merge(cursor: mariadb.Cursor, venue_id: int) -> Optional[Dict[str, Any]]:
    merge_cols = (
        "id, name, type, abbreviated_name, publisher_id, country_code, "
        "issn, eissn, homepage_url, aggregation_type, scopus_id, "
        "wikidata_id, openalex_id, scielo_id, mag_id, "
        "open_access, is_in_doaj, is_in_scielo, is_indexed_in_scopus"
    )
    cursor.execute(f"SELECT {merge_cols} FROM venues WHERE id = ? FOR UPDATE", (venue_id,))
    return row_to_dict(cursor, cursor.fetchone())


def merge_venues_python_fallback(
    cursor: mariadb.Cursor,
    primary_venue_id: int,
    secondary_venue_id: int,
    conflict_field: str,
) -> bool:
    if primary_venue_id == secondary_venue_id:
        return False

    primary = _get_venue_for_merge(cursor, primary_venue_id)
    if not primary:
        raise mariadb.OperationalError(f"Venue primário {primary_venue_id} não encontrado para merge.")

    secondary = _get_venue_for_merge(cursor, secondary_venue_id)
    if not secondary:
        log.info(
            "  -> Merge fallback (field=%s): venue secundário %s já não existe; tratando como unificado.",
            conflict_field,
            secondary_venue_id,
        )
        return True

    cursor.execute(
        "UPDATE publications SET venue_id = ? WHERE venue_id = ?",
        (primary_venue_id, secondary_venue_id),
    )
    moved_pubs = cursor.rowcount

    cursor.execute(
        """
        INSERT IGNORE INTO venue_subjects (venue_id, subject_id, score, source)
        SELECT ?, subject_id, score, source
        FROM venue_subjects
        WHERE venue_id = ?
        """,
        (primary_venue_id, secondary_venue_id),
    )
    merged_subjects = cursor.rowcount

    cursor.execute(
        """
        UPDATE venue_yearly_stats p
        JOIN venue_yearly_stats s
          ON s.year = p.year
         AND s.venue_id = ?
        SET
            p.works_count = COALESCE(p.works_count, 0) + COALESCE(s.works_count, 0),
            p.oa_works_count = COALESCE(p.oa_works_count, 0) + COALESCE(s.oa_works_count, 0),
            p.cited_by_count = COALESCE(p.cited_by_count, 0) + COALESCE(s.cited_by_count, 0)
        WHERE p.venue_id = ?
        """,
        (secondary_venue_id, primary_venue_id),
    )

    cursor.execute(
        """
        INSERT INTO venue_yearly_stats (venue_id, year, works_count, oa_works_count, cited_by_count)
        SELECT ?, s.year, s.works_count, s.oa_works_count, s.cited_by_count
        FROM venue_yearly_stats s
        LEFT JOIN venue_yearly_stats p
               ON p.venue_id = ?
              AND p.year = s.year
        WHERE s.venue_id = ?
          AND p.venue_id IS NULL
        """,
        (primary_venue_id, primary_venue_id, secondary_venue_id),
    )

    cursor.execute("DELETE FROM venues WHERE id = ?", (secondary_venue_id,))
    if cursor.rowcount == 0:
        log.warning(
            "  -> Merge fallback (field=%s): venue secundário %s já foi removido antes do DELETE.",
            conflict_field,
            secondary_venue_id,
        )

    update_fields: Dict[str, Any] = {}
    for field in VENUE_MERGE_FILL_IF_NULL_FIELDS:
        primary_val = primary.get(field)
        secondary_val = secondary.get(field)
        if primary_val is not None or secondary_val is None:
            continue

        if field in VENUE_MERGE_UNIQUE_FIELDS:
            cursor.execute(f"SELECT id FROM venues WHERE {field} = ? AND id <> ? LIMIT 1", (secondary_val, primary_venue_id))
            conflict = cursor.fetchone()
            if conflict:
                log.warning(
                    "  -> Merge fallback (field=%s): valor '%s' de %s não transferido para venue %s (já usado por venue %s).",
                    conflict_field,
                    secondary_val,
                    field,
                    primary_venue_id,
                    conflict[0],
                )
                continue

        update_fields[field] = secondary_val

    for field in VENUE_MERGE_BOOL_MAX_FIELDS:
        primary_v = int(bool(primary.get(field)))
        secondary_v = int(bool(secondary.get(field)))
        merged_v = max(primary_v, secondary_v)
        if primary.get(field) is None or merged_v != primary_v:
            update_fields[field] = merged_v

    if update_fields:
        clauses = ", ".join([f"`{k}` = ?" for k in update_fields])
        params = list(update_fields.values()) + [primary_venue_id]
        cursor.execute(
            f"UPDATE venues SET {clauses}, `updated_at` = NOW() WHERE id = ?",
            tuple(params),
        )

    log.warning(
        "  -> Duplicata unificada via fallback Python por %s: primary=%s <- secondary=%s (pubs:%s, subjects:%s).",
        conflict_field,
        primary_venue_id,
        secondary_venue_id,
        moved_pubs,
        merged_subjects,
    )
    return True


def filter_conflicting_updates(
    cursor: mariadb.Cursor,
    venue_id: int,
    venue_name: Optional[str],
    venue_type: Optional[str],
    upd_ovr: Dict[str, Any],
    upd_fil: Dict[str, Any],
    merge_duplicates: bool,
    prefer_db_merge_procedure: bool,
    dry_run: bool,
) -> bool:
    merged_any = False
    merged_secondary_ids = set()

    def attempt_merge(conflict_id: int, conflict_field: str) -> bool:
        nonlocal merged_any
        if conflict_id == venue_id:
            return False
        if conflict_id in merged_secondary_ids:
            return True
        if not merge_duplicates:
            return False

        if dry_run:
            log.warning(
                "[DRY-RUN]  -> Unificaria por conflito em %s: primary=%s <- secondary=%s.",
                conflict_field,
                venue_id,
                conflict_id,
            )
            return False

        if prefer_db_merge_procedure:
            try:
                cursor.execute("CALL sp_merge_single_venue_pair(?, ?)", (venue_id, conflict_id))
                try:
                    if cursor.description:
                        cursor.fetchall()
                except mariadb.Error:
                    pass
                while cursor.nextset():
                    try:
                        if cursor.description:
                            cursor.fetchall()
                    except mariadb.Error:
                        pass

                merged_secondary_ids.add(conflict_id)
                merged_any = True
                log.warning(
                    "  -> Duplicata unificada via procedure por conflito em %s: primary=%s <- secondary=%s.",
                    conflict_field,
                    venue_id,
                    conflict_id,
                )
                return True
            except mariadb.Error as e_merge:
                log.error(
                    "  -> Falha ao unificar duplicata via procedure (primary=%s, secondary=%s, field=%s): %s",
                    venue_id,
                    conflict_id,
                    conflict_field,
                    e_merge,
                )

        try:
            merge_venues_python_fallback(cursor, venue_id, conflict_id, conflict_field)
            merged_secondary_ids.add(conflict_id)
            merged_any = True
            return True
        except mariadb.Error as e_merge_fallback:
            log.error(
                "  -> Falha ao unificar duplicata via fallback Python (primary=%s, secondary=%s, field=%s): %s",
                venue_id,
                conflict_id,
                conflict_field,
                e_merge_fallback,
            )
            return False

    unique_fields = ['issn', 'eissn', 'openalex_id', 'wikidata_id', 'mag_id', 'scopus_id']
    for field in unique_fields:
        for target_name, target in (('OvW', upd_ovr), ('FiN', upd_fil)):
            if field not in target:
                continue
            conflict_id = find_conflict_id_by_unique_field(cursor, field, target[field], venue_id)
            if conflict_id:
                merge_ok = attempt_merge(conflict_id, field)
                target.pop(field, None)
                if merge_ok:
                    continue
                log.warning(
                    "  -> Ignorando %s em %s para venue %s: valor já pertence ao venue %s.",
                    field,
                    target_name,
                    venue_id,
                    conflict_id,
                )

    target_name = upd_ovr.get('name', venue_name)
    target_type = upd_ovr.get('type', venue_type)
    if target_name and target_type and ('name' in upd_ovr or 'type' in upd_ovr):
        conflict_id = has_name_type_conflict(cursor, target_name, target_type, venue_id)
        if conflict_id:
            merge_ok = attempt_merge(conflict_id, "name+type")
            if merge_ok:
                still_conflict = has_name_type_conflict(cursor, target_name, target_type, venue_id)
                if still_conflict:
                    upd_ovr.pop('name', None)
                    upd_ovr.pop('type', None)
                    log.warning(
                        "  -> Ignorando name/type em OvW para venue %s: combinação name+type ainda conflita com venue %s após merge.",
                        venue_id,
                        still_conflict,
                    )
            else:
                upd_ovr.pop('name', None)
                upd_ovr.pop('type', None)
                log.warning(
                    "  -> Ignorando name/type em OvW para venue %s: combinação name+type já pertence ao venue %s.",
                    venue_id,
                    conflict_id,
                )
    return merged_any


def create_new_venue(
    cursor: mariadb.Cursor,
    data: Dict[str, Any],
    args: argparse.Namespace,
) -> Tuple[Optional[int], bool]:
    new_venue = {}
    
    def prep_val(new, field):
        if new is None: return None
        try:
            if field in ['open_access','is_in_doaj','is_indexed_in_scopus']:
                return int(bool(new))
            elif field in ['cited_by_count','h_index','i10_index']:
                return int(new) if new is not None else None
            elif field=='2yr_mean_citedness':
                return float(new) if new is not None else None
            elif field in ['openalex_id', 'wikidata_id']:
                 return extract_identifier(new)
            elif field in ['issn', 'eissn']:
                return normalize_issn(new)
            val = str(new).strip()
            return val if val else None
        except (ValueError, TypeError, AttributeError):
            log.warning(f"Valor inválido '{new}' para campo {field} durante criação.")
            return None

    
    new_venue['name'] = prep_val(data.get('display_name'), 'name')
    if not new_venue['name']:
        log.warning(f"  -> JSON sem 'display_name' ({data.get('id', 'N/A')}). Pulando criação.")
        return None, False
    
    new_venue['homepage_url'] = prep_val(data.get('homepage_url'), 'homepage_url')
    new_venue['country_code'] = prep_val(data.get('country_code'), 'country_code')
    new_venue['open_access'] = prep_val(data.get('is_oa'), 'open_access')
    new_venue['is_in_doaj'] = prep_val(data.get('is_in_doaj'), 'is_in_doaj')
    
    j_issn_l, j_list = data.get('issn_l'), (data.get('issn') or [])
    new_venue['issn'] = prep_val(j_issn_l, 'issn')
    j_other = next((i for i in j_list if i and i != j_issn_l), None)
    new_venue['eissn'] = prep_val(j_other, 'eissn')

    ids = data.get('ids') or {}
    new_venue['openalex_id'] = prep_val(ids.get('openalex'), 'openalex_id')
    new_venue['wikidata_id'] = prep_val(ids.get('wikidata'), 'wikidata_id')
    new_venue['mag_id'] = prep_val(ids.get('mag'), 'mag_id')
    new_venue['is_indexed_in_scopus'] = prep_val(data.get('is_indexed_in_scopus'), 'is_indexed_in_scopus')
    
    summary = data.get('summary_stats') or {}
    new_venue['cited_by_count'] = prep_val(data.get('cited_by_count'), 'cited_by_count')
    new_venue['h_index'] = prep_val(summary.get('h_index'), 'h_index')
    new_venue['i10_index'] = prep_val(summary.get('i10_index'), 'i10_index')
    new_venue['2yr_mean_citedness'] = prep_val(summary.get('2yr_mean_citedness'), '2yr_mean_citedness')

    pid = None
    if pname := data.get('host_organization_name'):
        conn = cursor.connection
        cur_p = conn.cursor()
        try: 
            pid = get_or_create_publisher(cur_p, pname)
        except Exception as e:
            log.error(f"Erro ao obter/criar publisher '{pname}': {e}")
        finally: 
            cur_p.close()
    new_venue['publisher_id'] = pid
    
    new_venue['type'] = normalize_venue_type(data.get('type'))
    
    cols = []; vals = []; params = []
    for k, v in new_venue.items():
        if v is not None:
            cols.append(f"`{k}`")
            vals.append("?")
            params.append(v)
            
    if not cols: 
        log.warning(f"  -> Nenhum dado válido para criar venue de ({data.get('id', 'N/A')}).")
        return None, False

    query = f"INSERT INTO venues ({','.join(cols)}) VALUES ({','.join(vals)})"
    
    if not args.dry_run:
        existing_id, existing_key = recover_venue_id_after_conflict(cursor, new_venue)
        if existing_id:
            log.info(
                "  -> Venue já existente antes do INSERT (chave '%s'): ID %s",
                existing_key,
                existing_id,
            )
            return existing_id, False
        try:
            cursor.execute(query, tuple(params))
            new_id = cursor.lastrowid
            log.debug(f"  -> CRIADO Venue ID {new_id} ({new_venue['name']})")
            return new_id, True
        except mariadb.IntegrityError as e_int:
            log.warning(
                "Conflito ao CRIAR venue '%s' (OA:%s). %s. Recuperando...",
                new_venue.get('name'),
                new_venue.get('openalex_id'),
                e_int,
            )
            try:
                venue_id_recuperado, chave = recover_venue_id_after_conflict(cursor, new_venue)
                if venue_id_recuperado:
                    log.info(f"  -> Venue recuperado após conflito pela chave '{chave}': ID {venue_id_recuperado}")
                    return venue_id_recuperado, False
                log.error(f"FALHA CRÍTICA ao recuperar venue '{new_venue['name']}' após IntegrityError.")
                return None, False
            except mariadb.Error as e_retry:
                log.error(f"Erro DB ao recuperar venue '{new_venue['name']}' pós-conflito: {e_retry}")
                return None, False
        except mariadb.Error as e_ins:
             log.error(f"Erro DB ao CRIAR venue '{new_venue['name']}': {e_ins}")
             return None, False
    else:
        log.info(f"[DRY-RUN] -> Criaria Venue ({new_venue['name']}): Q:{query}|P:{tuple(params)}")
        return -1, True


def unificar_duplicatas_internas(conn: mariadb.Connection):
                                                                              
    log.info("--- Iniciando unificação interna (ISSN) ---"); cursor = None
    try:
        cursor = conn.cursor(); issn_map = {}
        cursor.execute("SELECT id, issn, eissn FROM venues WHERE issn IS NOT NULL OR eissn IS NOT NULL")
        for row in cursor.fetchall():
            vid, issn, eissn = row
            if issn: issn_map.setdefault(issn, []).append(vid)
            if eissn: issn_map.setdefault(eissn, []).append(vid)
        to_remove = set(); merge_pairs = []
        for issn, ids in issn_map.items():
            uids = sorted(list(set(ids)))
            if len(uids) > 1:
                pid = uids[0]
                for sid in uids[1:]:
                    if sid not in to_remove: log.warning(f"  -> Duplicata interna: ISSN '{issn}'. P:{pid} <- S:{sid}"); merge_pairs.append((pid, sid)); to_remove.add(sid)
        if not merge_pairs: log.info("Nenhuma duplicata interna (ISSN) encontrada."); return

        log.info(f"Encontradas {len(merge_pairs)} unificações internas."); merged_count = 0
        for pid, sid in merge_pairs:
            try:
                cursor.execute("UPDATE publications SET venue_id = ? WHERE venue_id = ?", (pid, sid)); pubs_moved = cursor.rowcount
                cursor.execute("INSERT IGNORE INTO venue_subjects (venue_id, subject_id, score, source) SELECT ?, subject_id, score, source FROM venue_subjects WHERE venue_id = ?", (pid, sid)); subs_merged = cursor.rowcount
                cursor.execute("DELETE FROM venues WHERE id = ?", (sid,)); deleted = cursor.rowcount
                if deleted > 0:
                    log.info(f"    -> Unificado {sid} -> {pid}. Pubs:{pubs_moved}, Subs:{subs_merged}.")
                    conn.commit()
                    merged_count += 1
                else:
                    log.warning(f"    -> Venue secundário {sid} não encontrado para remoção (já removido?).")
                    conn.rollback()
            except mariadb.Error as e: log.error(f"    -> Erro DB unificar {sid} -> {pid}: {e}"); conn.rollback()
        log.info(f"Unificações internas realizadas: {merged_count} de {len(merge_pairs)}.")
    except mariadb.Error as e: log.error(f"Erro geral unificação interna: {e}"); conn.rollback()
    finally:
        if cursor: cursor.close()
        log.info("--- Unificação interna concluída ---")

def unificar_venues_via_mapa(conn: mariadb.Connection, json_dir: str, arquivo_mapa="venue_merge_map.csv"):
                                                                                
    map_path = os.path.join(get_repo_root(), arquivo_mapa)
    if not os.path.exists(map_path): log.info(f"Mapa '{map_path}' não encontrado. Pulando."); return

    log.info(f"--- Iniciando unificação via mapa '{map_path}' ---"); processed, unified = 0, 0; cursor = None
    try:
        with open(map_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=1):
                processed += 1; id_inc_str, issn_corr = row.get('id_venue_incorreto'), row.get('issn_correto')
                if not id_inc_str or not issn_corr: log.warning(f"Linha {row_num} inválida. Pulando."); continue
                try: sid = int(id_inc_str)
                except ValueError: log.error(f"ID inválido '{id_inc_str}' linha {row_num}. Pulando."); continue

                if cursor is None: cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1 FROM venues WHERE id = ?", (sid,));
                    if not cursor.fetchone(): log.info(f"Mapa (L{row_num}): ID {sid} não existe. Pulando."); continue

                    cursor.execute("SELECT id FROM venues WHERE issn = ? OR eissn = ?", (issn_corr, issn_corr)); vp = cursor.fetchone(); pid = None
                    if not vp:
                        log.warning(f"Mapa (L{row_num}): Primário ISSN '{issn_corr}' não no DB. Tentando criar.");
                        issn_corr_nohyphen = issn_corr.replace('-', '')
                        json_filenames_to_try = [f"{issn_corr}.json", f"{issn_corr_nohyphen}.json"]
                        jpath = None
                        for fname in json_filenames_to_try:
                            potential_path = os.path.join(json_dir, fname)
                            if os.path.exists(potential_path):
                                jpath = potential_path
                                break

                        if not jpath: log.error(f"  -> JSON para ISSN '{issn_corr}' ou '{issn_corr_nohyphen}' não encontrado. Pulando."); continue

                        try:
                            with open(jpath, 'r', encoding='utf-8') as jf: data = json.load(jf)
                        except Exception as e: log.error(f"  -> Erro ler JSON '{jpath}': {e}. Pulando."); continue

                        nname = data.get('display_name');
                        if not nname: log.error(f"  -> 'display_name' não em '{jpath}'. Pulando."); continue

                        cursor.execute("SELECT id FROM venues WHERE name = ? AND (type = 'JOURNAL' OR type IS NULL)", (nname,)); ex_by_name = cursor.fetchone()
                        if ex_by_name:
                            pid = ex_by_name[0]; log.warning(f"  -> Venue nome '{nname}' já existe (ID:{pid}). Usando como primário.");
                            ni_l, ni_list = data.get('issn_l'), (data.get('issn') or [])
                            ni_e = next((i for i in ni_list if i and i != ni_l), None)
                            cursor.execute("UPDATE venues SET issn = COALESCE(issn, ?), eissn = COALESCE(eissn, ?) WHERE id = ?", (ni_l, ni_e, pid))
                        else:
                            ni_l, ni_list = data.get('issn_l'), (data.get('issn') or [])
                            ni_e = next((i for i in ni_list if i and i != ni_l), None)
                            p_name, p_id = data.get('host_organization_name'), None
                            if p_name:
                                cur_p=conn.cursor();
                                try: p_id=get_or_create_publisher(cur_p, p_name)
                                finally: cur_p.close()
                            log.info(f"  -> Criando venue primário: '{nname}' ISSN {issn_corr}")
                            cursor.execute("INSERT INTO venues (name, type, issn, eissn, publisher_id) VALUES (?, 'JOURNAL', ?, ?, ?)", (nname, ni_l if ni_l else issn_corr, ni_e, p_id));
                            pid=cursor.lastrowid; log.info(f"  -> Novo venue ID {pid}.")
                    else: pid = vp[0]

                    if not pid: log.error(f"Mapa (L{row_num}): Falha determinar ID primário '{issn_corr}'. Pulando."); continue
                    if pid == sid: log.info(f"Mapa (L{row_num}): IDs iguais ({pid}). Pulando."); continue

                    log.warning(f"Unificando ID {sid} -> ID {pid} (ISSN:{issn_corr}, L{row_num}).")
                    cursor.execute("UPDATE publications SET venue_id = ? WHERE venue_id = ?", (pid, sid)); pubs_moved = cursor.rowcount
                    cursor.execute("INSERT IGNORE INTO venue_subjects (venue_id, subject_id, score, source) SELECT ?, subject_id, score, source FROM venue_subjects WHERE venue_id = ?", (pid, sid)); subs_merged = cursor.rowcount
                    cursor.execute("DELETE FROM venues WHERE id = ?", (sid,)); deleted = cursor.rowcount
                    if deleted > 0:
                         log.info(f"  -> Unificado. Pubs:{pubs_moved}, Subs:{subs_merged}. Venue {sid} removido.")
                         conn.commit(); unified += 1
                    else:
                        log.warning(f"  -> Venue secundário {sid} não encontrado para remoção.")
                        conn.rollback()
                except mariadb.Error as e: log.error(f"Erro DB (L{row_num}): {e}"); conn.rollback()
                except Exception as e: log.error(f"Erro Inesperado (L{row_num}): {e}", exc_info=True); conn.rollback()
    except FileNotFoundError: log.error(f"Mapa '{map_path}' não encontrado.")
    except Exception as e: log.error(f"Erro ler mapa '{map_path}': {e}", exc_info=True)
    finally:
        if cursor: cursor.close()
    log.info(f"--- Unificação via mapa concluída. {processed} linhas, {unified} unificações. ---")


def enrich_venues(conn: mariadb.Connection, json_files: List[str], args: argparse.Namespace):

    if args.dry_run: log.warning("DRY-RUN ativado.")

    unique_json_files = sorted(set(json_files))
    total = len(unique_json_files)
    log.info(f"--- OpenAlex venue ingest: {total} files ---")

    counters = {'updated': 0, 'created': 0, 'errors': 0}

    for i, json_path in enumerate(unique_json_files, start=1):

        if args.limit and i > args.limit:
            log.info(f"Limite ({args.limit}) atingido.")
            break

        prefix = f"[{i}/{total}]"
        venue_label = os.path.basename(json_path)
        outcome = None
        cursor_update = None

        try:
            with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
            venue_label = data.get('display_name') or data.get('id', venue_label)

            cursor_update = conn.cursor()

            existing_venue_data = find_existing_venue(cursor_update, data)

            if existing_venue_data:

                venue = existing_venue_data
                venue_id = venue['id']
                venue_label = f"{venue_id} {venue.get('name') or data.get('display_name', '')}"

                upd_ovr, upd_fil = {}, {}
                def check_add(new, old, field, strategy='overwrite'):
                    nonlocal upd_ovr, upd_fil
                    new_s = str(new).strip() if new is not None else None; old_s = str(old).strip() if old is not None else None
                    if new_s is None or new_s == '': return
                    should_update = (strategy == 'overwrite') or                                    (strategy == 'fill_if_null' and (old is None or str(old).strip() == ''))

                    if should_update and new_s != old_s:
                        target = upd_ovr if strategy=='overwrite' else upd_fil
                        orig = new
                        try:
                            if field in ['open_access','is_in_doaj','is_indexed_in_scopus']:
                                new = int(bool(new))
                            elif field in ['cited_by_count','h_index','i10_index']:
                                try: new = int(new) if new is not None else None
                                except (ValueError, TypeError): new = None
                            elif field=='2yr_mean_citedness':
                                try: new = float(new) if new is not None else None
                                except (ValueError, TypeError): new = None
                            elif field in ['issn', 'eissn']:
                                new = normalize_issn(new)

                            if new is not None or field in ['name','homepage_url','country_code','issn','eissn','openalex_id','wikidata_id','mag_id','publisher_id', 'cited_by_count', 'h_index', 'i10_index', '2yr_mean_citedness']:
                                target[field] = new
                        except (ValueError, TypeError) as e:
                            log.warning(f"  -> Erro conversão '{orig}' campo {field} (ID {venue_id}): {e}.")

                check_add(data.get('display_name'), venue.get('name'), 'name', 'fill_if_null')
                # Quando aggregation_type já existe no DB (ex.: Scopus), ele tem precedência para evitar ping-pong de type.
                existing_agg_type = venue.get('aggregation_type')
                if existing_agg_type:
                    canonical_type = normalize_venue_type(existing_agg_type)
                    if canonical_type != VENUE_TYPE_OTHER:
                        check_add(canonical_type, venue.get('type'), 'type')
                else:
                    raw_type = data.get('type')
                    if raw_type:
                        check_add(normalize_venue_type(raw_type), venue.get('type'), 'type')
                check_add(data.get('homepage_url'), venue.get('homepage_url'), 'homepage_url')
                check_add(data.get('country_code'), venue.get('country_code'), 'country_code')
                check_add(data.get('is_oa'), venue.get('open_access'), 'open_access')
                check_add(data.get('is_in_doaj'), venue.get('is_in_doaj'), 'is_in_doaj')
                j_issn_l, j_list = data.get('issn_l'), (data.get('issn') or [])
                j_other = next((ii for ii in j_list if ii and ii != j_issn_l), None)
                check_add(j_issn_l, venue.get('issn'), 'issn', 'fill_if_null')
                if j_other and j_other != venue.get('issn'): check_add(j_other, venue.get('eissn'), 'eissn', 'fill_if_null')

                ids = data.get('ids') or {}
                if openalex_id := extract_identifier(ids.get('openalex')):
                    check_add(openalex_id, venue.get('openalex_id'), 'openalex_id', 'fill_if_null')
                if wikidata_id := extract_identifier(ids.get('wikidata')):
                    check_add(wikidata_id, venue.get('wikidata_id'), 'wikidata_id', 'fill_if_null')
                check_add(ids.get('mag'), venue.get('mag_id'), 'mag_id', 'fill_if_null')
                check_add(data.get('is_indexed_in_scopus'), venue.get('is_indexed_in_scopus'), 'is_indexed_in_scopus')

                if pname := data.get('host_organization_name'):
                    cur_p = conn.cursor(); pid = None;
                    try: pid = get_or_create_publisher(cur_p, pname);
                    finally: cur_p.close()
                    if pid is not None: check_add(pid, venue.get('publisher_id'), 'publisher_id', 'fill_if_null')

                summary = data.get('summary_stats') or {}
                check_add(data.get('cited_by_count'), venue.get('cited_by_count'), 'cited_by_count', 'fill_if_null')
                check_add(summary.get('h_index'), venue.get('h_index'), 'h_index', 'fill_if_null')
                check_add(summary.get('i10_index'), venue.get('i10_index'), 'i10_index', 'fill_if_null')
                check_add(summary.get('2yr_mean_citedness'), venue.get('2yr_mean_citedness'), '2yr_mean_citedness', 'fill_if_null')

                updated_this_venue = False
                merged_any = False
                if upd_ovr or upd_fil:
                    merged_any = filter_conflicting_updates(
                        cursor_update,
                        venue['id'],
                        venue.get('name'),
                        venue.get('type'),
                        upd_ovr,
                        upd_fil,
                        merge_duplicates=not args.no_merge_duplicates,
                        prefer_db_merge_procedure=args.prefer_db_merge_procedure,
                        dry_run=args.dry_run,
                    )
                    if merged_any and not args.dry_run:
                        updated_this_venue = True
                    clauses, params = [], []
                    if upd_ovr: clauses.extend([f"`{k}`=?" for k in upd_ovr]); params.extend(upd_ovr.values())
                    if upd_fil: clauses.extend([f"`{k}`=COALESCE(`{k}`,?)" for k in upd_fil]); params.extend(upd_fil.values())
                    if clauses:
                        params.append(venue['id']); query = f"UPDATE venues SET {','.join(clauses)} WHERE id=?"

                        if not args.dry_run:
                            cursor_update.execute(query, tuple(params))
                            if cursor_update.rowcount > 0:
                                updated_this_venue = True
                        else:
                            updated_this_venue = True

                linked = 0
                if not args.dry_run:
                    linked = update_venue_subjects_hierarchical(cursor_update, venue['id'], data.get('topics', []))
                    conn.commit()
                    if updated_this_venue: counters['updated'] += 1
                elif updated_this_venue:
                    counters['updated'] += 1

                # Build outcome detail
                n_ovr = len(upd_ovr)
                n_fil = len(upd_fil)
                if updated_this_venue:
                    parts = []
                    if n_ovr:
                        parts.append(f"ovw:{n_ovr}")
                    if n_fil:
                        parts.append(f"fin:{n_fil}")
                    if merged_any:
                        parts.append("merge")
                    if linked:
                        parts.append(f"+{linked}s")
                    detail = ", ".join(parts) if parts else ""
                    outcome = f"Updated ({detail})" if detail else "Updated"
                else:
                    outcome = "OK"

            else:

                new_venue_id, was_created = create_new_venue(cursor_update, data, args)

                if new_venue_id is not None:
                    linked = 0
                    if not args.dry_run:
                        linked = update_venue_subjects_hierarchical(cursor_update, new_venue_id, data.get('topics', []))
                        conn.commit()
                        if was_created:
                            counters['created'] += 1
                        else:
                            counters['updated'] += 1
                    elif new_venue_id == -1:
                        counters['created'] += 1

                    venue_label = f"{new_venue_id} {data.get('display_name', '')}"
                    detail = f" +{linked}s" if linked else ""
                    outcome = f"Created{detail}" if was_created else f"Updated (recovered){detail}"
                else:
                    conn.rollback()
                    outcome = "Error (create failed)"

        except FileNotFoundError:
            outcome = "Error: file not found"
        except json.JSONDecodeError:
            outcome = "Error: invalid JSON"
        except mariadb.Error as e_db:
            outcome = f"Error: {e_db}"
            conn.rollback()
        except Exception as e_gen:
            outcome = f"Error: {e_gen}"
            log.debug(f"Traceback for {json_path}:", exc_info=True)
            conn.rollback()
        finally:
            if cursor_update: cursor_update.close()
            if outcome:
                log.info(f"{prefix} {venue_label} → {outcome}")
            if outcome and outcome.startswith("Error"):
                counters['errors'] += 1

    log.info(f"=== Finished: {counters} ===")



def main():
    parser = argparse.ArgumentParser(description="Unifica, verifica e enriquece venues.")
    parser.add_argument('--json-dir', required=True, help="Diretório com JSONs OpenAlex.")
    parser.add_argument('--batch-size', type=int, default=100, help="Venues por lote. (Não usado na nova lógica JSON-first, mas mantido por args)")
    parser.add_argument('--limit', type=int, default=None, help="Limite total de arquivos JSON.")
    parser.add_argument('--dry-run', action='store_true', help="Não salva alterações.")
    parser.add_argument('--no-merge-duplicates', action='store_true', help="Desativa unificação automática de duplicatas quando houver conflito de chave única.")
    parser.add_argument('--prefer-db-merge-procedure', action='store_true', help="Tenta usar sp_merge_single_venue_pair antes do fallback Python para unificação de duplicatas.")
    args = parser.parse_args()

    conn = None
    try:
        conn = get_connection()

        if not args.dry_run:
            unificar_duplicatas_internas(conn)
            unificar_venues_via_mapa(conn, args.json_dir)
        else: log.warning("DRY-RUN: Unificação pulada.")

        json_files = collect_json_files(args.json_dir)
        if not json_files:
            log.error("Nenhum JSON mapeado. Verifique diretório.")
            return

        enrich_venues(conn, json_files, args)

    except Exception as e:
        log.critical(f"Falha Crítica no processo principal: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                log.info("Rollback final realizado devido a erro.")
            except mariadb.Error as rb_err:
                log.error(f"Erro no rollback final: {rb_err}")
    finally:
        if conn:
            try:
                 conn.autocommit = True
                 conn.close()
                 log.info("Processo finalizado. Conexão fechada.")
            except mariadb.Error as close_err:
                 log.error(f"Erro ao fechar conexão: {close_err}")
        else:
             log.info("Processo finalizado (conexão não estabelecida ou já fechada).")

if __name__ == '__main__':
    main()
