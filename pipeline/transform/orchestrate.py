#!/usr/bin/env python3
"""
Orchestrate the full transform workflow in the official execution order.

This script is the single entrypoint for running transform end-to-end with
preflight checks and deterministic step sequencing.
"""

import argparse
import logging
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Set

import mariadb

from pipeline.common import get_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


ROOT_DIR = Path(__file__).resolve().parents[2]
TRANSFORM_DIR = ROOT_DIR / "pipeline" / "transform"


@dataclass(frozen=True)
class Step:
    step_id: str
    description: str
    kind: str  # "python" | "sql"
    file_path: Path
    args: Sequence[str]
    required_procedures: Sequence[str] = ()


ALL_STEP_IDS = [
    "setup_procedures",
    "cleanup",
    "structural_cleanup",
    "pre_dedup_metrics",
    "deduplicate_works",
    "persons_full_process",
    "post_dedup_metrics",
    "venue_ranking_setup",
]


DELETE_FROM_WORKS_PROCS = (
    "sp_clean_inconsistent_data",
    "sp_clean_orphaned_data",
    "sp_clean_orphaned_persons",
    "sp_review_reference_consistency",
)

PERSONS_FULL_PROCS = (
    "sp_merge_persons_in_batches",
    "sp_generate_person_signatures",
    "sp_fix_person_name_parts",
)

METRICS_PROCS = (
    "sp_update_persons_summary",
    "sp_update_10yr_impact_factors",
    "sp_populate_sphinx_venues_summary",
    "sp_update_work_author_summary_all",
    "sp_update_work_subjects_summary_all",
    "sp_update_works_summary",
)


def _quote_cmd(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(c) for c in cmd)


def build_steps(config_path: str | None) -> List[Step]:
    config_arg = ["--config", config_path] if config_path else []
    return [
        Step(
            step_id="setup_procedures",
            description="Create/update stored procedures used by transform",
            kind="sql",
            file_path=TRANSFORM_DIR / "procedures.sql",
            args=(),
        ),
        Step(
            step_id="cleanup",
            description="Run cleanup.py profile + cleanup phases",
            kind="python",
            file_path=TRANSFORM_DIR / "cleanup.py",
            args=(*config_arg, "--mode", "both"),
        ),
        Step(
            step_id="structural_cleanup",
            description="Execute delete_from_works.sql",
            kind="sql",
            file_path=TRANSFORM_DIR / "delete_from_works.sql",
            args=(),
            required_procedures=DELETE_FROM_WORKS_PROCS,
        ),
        Step(
            step_id="pre_dedup_metrics",
            description="Run metrics pre-dedup (works + sphinx)",
            kind="python",
            file_path=TRANSFORM_DIR / "metrics.py",
            args=(*config_arg, "--full", "--works", "--sphinx"),
            required_procedures=METRICS_PROCS,
        ),
        Step(
            step_id="deduplicate_works",
            description="Execute work deduplication SQL",
            kind="sql",
            file_path=TRANSFORM_DIR / "deduplicate_works.sql",
            args=(),
        ),
        Step(
            step_id="persons_full_process",
            description="Execute persons_full_process.sql",
            kind="sql",
            file_path=TRANSFORM_DIR / "persons_full_process.sql",
            args=(),
            required_procedures=PERSONS_FULL_PROCS,
        ),
        Step(
            step_id="post_dedup_metrics",
            description="Run definitive post-dedup metrics (full)",
            kind="python",
            file_path=TRANSFORM_DIR / "metrics.py",
            args=(*config_arg, "--full"),
            required_procedures=METRICS_PROCS,
        ),
        Step(
            step_id="venue_ranking_setup",
            description="Recreate venue ranking structures/procedures and recalc ranking",
            kind="sql",
            file_path=TRANSFORM_DIR / "venue_ranking_setup.sql",
            args=(),
        ),
    ]


def select_steps(
    all_steps: Sequence[Step],
    from_step: str | None,
    to_step: str | None,
    skip_procedures: bool,
) -> List[Step]:
    steps = list(all_steps)
    ids = [s.step_id for s in steps]
    start = ids.index(from_step) if from_step else 0
    end = ids.index(to_step) if to_step else len(steps) - 1
    if start > end:
        raise ValueError("--from-step must come before --to-step")
    selected = steps[start : end + 1]
    if skip_procedures:
        selected = [s for s in selected if s.step_id != "setup_procedures"]
    if not selected:
        raise ValueError("No steps selected after filters.")
    return selected


def ensure_files_exist(steps: Sequence[Step]) -> None:
    missing = [str(s.file_path) for s in steps if not s.file_path.exists()]
    if missing:
        raise FileNotFoundError("Missing transform files:\n" + "\n".join(missing))


def ensure_binaries_exist(python_bin: str, mariadb_bin: str) -> None:
    if shutil.which(python_bin) is None:
        raise FileNotFoundError(f"Python binary not found in PATH: {python_bin}")
    if shutil.which(mariadb_bin) is None:
        raise FileNotFoundError(f"MariaDB binary not found in PATH: {mariadb_bin}")


def collect_required_procedures(steps: Sequence[Step]) -> Set[str]:
    required: Set[str] = set()
    for step in steps:
        required.update(step.required_procedures)
    return required


def check_required_procedures(conn: mariadb.Connection, required: Iterable[str]) -> None:
    required = sorted(set(required))
    if not required:
        return
    placeholders = ",".join(["?"] * len(required))
    query = (
        "SELECT ROUTINE_NAME "
        "FROM information_schema.ROUTINES "
        "WHERE ROUTINE_SCHEMA = DATABASE() "
        "  AND ROUTINE_TYPE = 'PROCEDURE' "
        f"  AND ROUTINE_NAME IN ({placeholders})"
    )
    cur = conn.cursor()
    cur.execute(query, tuple(required))
    installed = {row[0] for row in cur.fetchall()}
    cur.close()
    missing = [name for name in required if name not in installed]
    if missing:
        raise RuntimeError(
            "Missing required procedures: "
            + ", ".join(missing)
            + ". Run setup_procedures first."
        )


def precheck_dedup_inputs(conn: mariadb.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM work_author_summary")
    work_author_rows = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM sphinx_works_summary")
    sphinx_rows = int(cur.fetchone()[0] or 0)
    cur.close()
    if work_author_rows == 0 or sphinx_rows == 0:
        raise RuntimeError(
            "deduplicate_works requires non-empty work_author_summary and "
            "sphinx_works_summary (run pre_dedup_metrics first)."
        )


def run_python_step(step: Step, python_bin: str, dry_run: bool) -> None:
    cmd = [python_bin, str(step.file_path), *step.args]
    log.info("cmd: %s", _quote_cmd(cmd))
    if dry_run:
        return
    subprocess.run(cmd, cwd=str(ROOT_DIR), check=True)


def run_sql_step(step: Step, mariadb_bin: str, database: str, dry_run: bool) -> None:
    cmd = [mariadb_bin, database]
    log.info("cmd: %s < %s", _quote_cmd(cmd), step.file_path)
    if dry_run:
        return
    with open(step.file_path, "rb") as handle:
        subprocess.run(cmd, cwd=str(ROOT_DIR), stdin=handle, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run pipeline/transform in the official execution order.",
    )
    parser.add_argument("--config", type=str, help="Path to config.ini")
    parser.add_argument("--python-bin", default=sys.executable, help="Python binary for transform Python scripts")
    parser.add_argument("--mariadb-bin", default="mariadb", help="MariaDB client binary")
    parser.add_argument("--database", default="data", help="Database name")
    parser.add_argument("--from-step", choices=ALL_STEP_IDS, help="Start execution at this step")
    parser.add_argument("--to-step", choices=ALL_STEP_IDS, help="Stop execution at this step")
    parser.add_argument("--skip-procedures", action="store_true", help="Skip setup_procedures step")
    parser.add_argument("--skip-db-preflight", action="store_true", help="Skip DB connectivity/procedure checks")
    parser.add_argument("--skip-procedure-check", action="store_true", help="Skip required procedure existence checks")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--list-steps", action="store_true", help="Print selected steps and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()

    all_steps = build_steps(args.config)
    steps = select_steps(all_steps, args.from_step, args.to_step, args.skip_procedures)
    ensure_files_exist(steps)
    ensure_binaries_exist(args.python_bin, args.mariadb_bin)

    if args.list_steps:
        for idx, step in enumerate(steps, 1):
            print(f"{idx}. {step.step_id} - {step.description}")
        return

    conn = None
    try:
        if not args.skip_db_preflight:
            conn = get_connection(args.config)
            cur = conn.cursor()
            cur.execute("SELECT DATABASE(), VERSION()")
            db_name, db_version = cur.fetchone()
            cur.close()
            log.info("DB preflight: schema=%s version=%s", db_name, db_version)

            if not args.skip_procedure_check:
                required = collect_required_procedures(steps)
                # If setup_procedures is selected, downstream checks happen after it runs.
                if "setup_procedures" not in [s.step_id for s in steps]:
                    check_required_procedures(conn, required)

        for idx, step in enumerate(steps, 1):
            log.info("=== [%d/%d] %s ===", idx, len(steps), step.step_id)
            log.info(step.description)
            t0 = time.time()

            if step.kind == "python":
                run_python_step(step, args.python_bin, args.dry_run)
            elif step.kind == "sql":
                if (
                    step.step_id == "deduplicate_works"
                    and conn is not None
                    and not args.skip_db_preflight
                    and not args.dry_run
                ):
                    precheck_dedup_inputs(conn)
                run_sql_step(step, args.mariadb_bin, args.database, args.dry_run)
            else:
                raise RuntimeError(f"Unknown step kind: {step.kind}")

            if (
                conn is not None
                and not args.skip_db_preflight
                and not args.skip_procedure_check
                and step.step_id == "setup_procedures"
                and not args.dry_run
            ):
                required = collect_required_procedures(steps)
                check_required_procedures(conn, required)

            log.info("step complete in %.1fs", time.time() - t0)

        log.info("Transform orchestration complete in %.1fs", time.time() - started)
    finally:
        if conn is not None:
            try:
                conn.close()
            except mariadb.Error:
                pass


if __name__ == "__main__":
    main()
