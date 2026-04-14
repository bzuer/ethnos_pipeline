"""Shared main loop for work loaders.

Encapsulates folder iteration, savepoint/batch commit logic, reconnection,
cache management, DOI skip/mark, and status summary reporting.

Both Crossref and OpenAlex work loaders delegate to run_work_loader() with
source-specific callbacks for file parsing, exclusion checking, and record
processing.
"""

import os
import logging
import argparse
from datetime import datetime
from typing import Callable, Dict, Optional, Tuple

import mariadb

from pipeline.load.db import get_connection, ensure_connection, safe_rollback
from pipeline.load.io_utils import build_cache, discover_input_folders, collect_json_files
from pipeline.load.constants import (
    STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE,
    STATUS_SKIPPED, STATUS_ERROR,
)
from pipeline.load.works.shared import (
    should_skip_existing_doi, mark_doi_processed,
    bulk_filter_new_files, bulk_filter_update_files,
)


def _accumulate_status(status, counters):
    """Increment the appropriate counter for a status."""
    if status == STATUS_INSERTED:
        counters['inserted'] += 1
    elif status == STATUS_UPDATED:
        counters['updated'] += 1
    elif status == STATUS_NO_CHANGE:
        counters['validated'] += 1
    elif status == STATUS_SKIPPED:
        counters['skipped'] += 1
    elif status == STATUS_ERROR:
        counters['errors'] += 1


def run_work_loader(
    args: argparse.Namespace,
    *,
    parse_file: Callable,
    get_exclusion_reason: Callable,
    process_record_no_tx: Callable,
    process_record_per_file: Callable,
):
    """Shared main loop for work loaders.

    Callbacks:
        parse_file(filepath) -> (message, doi, parse_status)
            Returns parsed JSON, DOI, and optional parse_status (None=ok, STATUS_SKIPPED, STATUS_ERROR).
            OpenAlex may return extra fields bundled in message dict.

        get_exclusion_reason(message) -> Optional[str]
            Returns exclusion reason string or None if the record should be loaded.

        process_record_no_tx(cursor, filepath, message, doi, mode, cache) -> str
            Processes a single record within an existing transaction (savepoint mode).
            Returns STATUS_* string.

        process_record_per_file(conn, filepath, message, doi, mode, cache) -> str
            Processes a single record with its own transaction (per-file mode).
            Returns STATUS_* string.
    """
    conn = None
    counters = {'inserted': 0, 'updated': 0, 'validated': 0, 'skipped': 0, 'errors': 0}
    total_processed_files = 0
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
        cache = build_cache()

        for subdir_path in subdirectories:
            folder_name = os.path.basename(subdir_path)
            all_json_files = collect_json_files(subdir_path, args.limit)
            total_files_in_folder = len(all_json_files)

            # --mode fast/update: bulk-filter via filename DOIs before any JSON parsing
            if args.mode in ("fast", "update") and total_files_in_folder > 0:
                pre_cursor = conn.cursor()
                if args.mode == "fast":
                    all_json_files, pre_skipped = bulk_filter_new_files(
                        pre_cursor, all_json_files, doi_presence_cache,
                    )
                else:
                    all_json_files, pre_skipped = bulk_filter_update_files(
                        pre_cursor, all_json_files, doi_presence_cache,
                    )
                pre_cursor.close()
                counters['skipped'] += pre_skipped
                total_files_in_folder = len(all_json_files)
                if total_files_in_folder == 0:
                    label = "FAST" if args.mode == "fast" else "UPDATE"
                    reason = "all DOIs exist" if args.mode == "fast" else "no existing DOIs"
                    logging.info(f"{label} folder={folder_name} {reason}, skipping entirely")
                    continue

            logging.info(f"Processing {total_files_in_folder} files in {folder_name}...")

            if args.commit_batch and args.commit_batch > 0:
                folder_commit_batch = max(1, min(args.commit_batch, total_files_in_folder or 1))
                logging.info(f"COMMIT mode=fixed folder={folder_name} batch={folder_commit_batch}")
            else:
                folder_commit_batch = max(1, total_files_in_folder)
                logging.info(f"COMMIT mode=per-folder folder={folder_name} batch={folder_commit_batch}")

            if folder_commit_batch > 1:
                conn, counters = _run_batch_mode(
                    conn, args, all_json_files, total_files_in_folder,
                    folder_name, folder_commit_batch, cache, cache_reset_interval,
                    doi_presence_cache, counters,
                    parse_file=parse_file,
                    get_exclusion_reason=get_exclusion_reason,
                    process_record_no_tx=process_record_no_tx,
                )
            else:
                conn, counters = _run_per_file_mode(
                    conn, args, all_json_files, total_files_in_folder,
                    folder_name, cache, cache_reset_interval,
                    doi_presence_cache, counters,
                    parse_file=parse_file,
                    get_exclusion_reason=get_exclusion_reason,
                    process_record_per_file=process_record_per_file,
                )

            total_processed_files += total_files_in_folder

    except Exception as e:
        logging.critical(f"Critical main execution failure: {e}", exc_info=True)
    finally:
        if conn and conn.open:
            conn.close()
        elapsed = (datetime.now() - script_start_time).total_seconds()
        logging.info(f"Total time: {elapsed:.2f} sec. Processed {total_processed_files} files.")
        logging.info(
            "Summary: "
            f"Inserted={counters['inserted']}, Updated={counters['updated']}, "
            f"Validated={counters['validated']}, Skipped={counters['skipped']}, "
            f"Errors={counters['errors']}"
        )


def _parse_and_filter(filepath, cursor, args, doi_presence_cache, counters, parse_file, get_exclusion_reason):
    """Parse file, check exclusion, check DOI skip. Returns (message, doi, should_continue)."""
    result = parse_file(filepath)
    # parse_file returns (message, doi, parse_status) — unpack
    message, doi, parse_status = result[0], result[-2], result[-1]

    if parse_status:
        if parse_status == STATUS_SKIPPED:
            counters['skipped'] += 1
        else:
            counters['errors'] += 1
        return None, None, False

    if get_exclusion_reason(message):
        counters['skipped'] += 1
        return None, None, False

    if should_skip_existing_doi(cursor, doi, args.mode, doi_presence_cache):
        counters['skipped'] += 1
        return None, None, False

    return message, doi, True


def _run_batch_mode(conn, args, all_json_files, total_files_in_folder,
                    folder_name, folder_commit_batch, cache, cache_reset_interval,
                    doi_presence_cache, counters, *,
                    parse_file, get_exclusion_reason, process_record_no_tx):
    """SAVEPOINT-based batch commit mode."""
    batch_count = 0
    conn.begin()
    cursor = conn.cursor()

    for k, filepath in enumerate(all_json_files, 1):
        if k % 500 == 0:
            logging.info(f"FILE progress {k}/{total_files_in_folder}")
        if cache_reset_interval and k % cache_reset_interval == 0:
            cache = build_cache()
            logging.info(f"CACHE reset interval={cache_reset_interval} folder={folder_name}")

        message, doi, should_continue = _parse_and_filter(
            filepath, cursor, args, doi_presence_cache, counters,
            parse_file, get_exclusion_reason,
        )
        if not should_continue:
            continue

        was_open = conn.open
        savepoint_name = f"sp_{k}"
        force_reconnect = False
        transaction_reset = False
        status = STATUS_ERROR

        try:
            cursor.execute(f"SAVEPOINT {savepoint_name}")
            status = process_record_no_tx(cursor, filepath, message, doi, args.mode, cache)
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
            detail = str(e) or type(e).__name__
            logging.error(f"FILE {os.path.basename(filepath)} error={detail}")
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

        _accumulate_status(status, counters)
        mark_doi_processed(doi, args.mode, status, doi_presence_cache)

    if batch_count > 0:
        conn.commit()
    try:
        cursor.close()
    except mariadb.Error:
        pass

    return conn, counters


def _run_per_file_mode(conn, args, all_json_files, total_files_in_folder,
                       folder_name, cache, cache_reset_interval,
                       doi_presence_cache, counters, *,
                       parse_file, get_exclusion_reason, process_record_per_file):
    """Per-file transaction mode."""
    check_cursor = conn.cursor()

    for k, filepath in enumerate(all_json_files, 1):
        if k % 500 == 0:
            logging.info(f"FILE progress {k}/{total_files_in_folder}")
        if cache_reset_interval and k % cache_reset_interval == 0:
            cache = build_cache()
            logging.info(f"CACHE reset interval={cache_reset_interval} folder={folder_name}")

        message, doi, should_continue = _parse_and_filter(
            filepath, check_cursor, args, doi_presence_cache, counters,
            parse_file, get_exclusion_reason,
        )
        if not should_continue:
            continue

        was_open = conn.open
        status = process_record_per_file(conn, filepath, message, doi, args.mode, cache)
        if not conn.open and was_open:
            conn = ensure_connection(conn, args.config)
            try:
                check_cursor.close()
            except mariadb.Error:
                pass
            check_cursor = conn.cursor()

        _accumulate_status(status, counters)
        mark_doi_processed(doi, args.mode, status, doi_presence_cache)

    try:
        check_cursor.close()
    except mariadb.Error:
        pass

    return conn, counters


def add_work_loader_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the standard CLI arguments shared by all work loaders."""
    parser.add_argument("directory", help="Root directory with subfolders containing .json files.")
    parser.add_argument("--mode", choices=["new", "full", "fast", "update"], default="new",
                        help="Ingest mode: new=only new DOIs, full=all records, fast=bulk DOI pre-check (no JSON parse for existing), update=only update publications already in DB (no inserts).")
    parser.add_argument("--limit", type=int,
                        help="Limit files processed per folder.")
    parser.add_argument("--commit-batch", type=int, default=250,
                        help="Commit every N files. Use 0 to commit once per subfolder.")
    parser.add_argument("--cache-reset-interval", type=int, default=0,
                        help="Reset in-memory cache every N files (0 to disable).")
    parser.add_argument("--config", type=str,
                        help="Path to config.ini (defaults to script dir or repo root).")
