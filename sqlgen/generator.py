"""
sqlgen/generator.py

Main orchestrator for SQL script generation.

Entry point: generate(params) → GenerationResult

Reads Phase 1 findings JSON and user-provided parameters, then:
  1. Generates {client}_config.sql        via config_sql.py
  2. Generates per-source load sproc(s)   via load_sproc.py
  3. Generates {client}_liquibase.xml     via liquibase_xml.py
  4. Writes all files to output/{client}/sqlgen/
  5. Returns paths + summary metadata
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from . import config_sql, liquibase_xml, load_sproc
from .constants import (
    LOADED_FOLDER_TEMPLATE,
    SFTP_FOLDER_TEMPLATE,
    SOURCE_TO_DEFAULT_DS_NUMBER,
    SOURCE_TO_ENTITY_NAME,
)


@dataclass
class FileParams:
    """User-provided parameters for a single source file."""

    source: str                  # Phase 1 source type key (e.g. 'payroll')
    ds_number: int               # Data source number (e.g. 2)
    source_name: str             # Human-readable name (e.g. 'Payroll')
    sftp_folder: str             # Full SFTP path to the 'To Load' folder
    loaded_folder: str           # Full SFTP path to the 'Loaded' folder
    file_name_pattern: str       # File name match pattern (e.g. '%Payroll%')
    row_terminator: str = "0x0a"
    automated_load: bool = False
    daily_load: bool = False


@dataclass
class GenerationParams:
    """All parameters needed to run SQL generation for one client."""

    client_id: str               # 4-digit (e.g. '0073')
    client_name: str             # (e.g. 'Ardent')
    raw_database: str            # (e.g. '0073_Ardent_Raw')
    files: list[FileParams]
    output_dir: str | None = None  # Override default output path


@dataclass
class GenerationResult:
    """Paths and metadata for all generated files."""

    config_sql_path: str
    sproc_paths: list[str]
    liquibase_xml_path: str
    summary_path: str
    output_dir: str
    generated_at: str
    warnings: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def generate(params: GenerationParams, phase1_path: str) -> GenerationResult:
    """
    Run full SQL generation for a client.

    Parameters
    ----------
    params       : GenerationParams with user-provided values
    phase1_path  : Absolute path to phase1_findings.json

    Returns
    -------
    GenerationResult with paths to all written files.
    """
    with open(phase1_path, encoding="utf-8") as fh:
        phase1 = json.load(fh)

    # Resolve output directory
    if params.output_dir:
        out_dir = Path(params.output_dir)
    else:
        findings_dir = Path(phase1_path).parent
        out_dir = findings_dir / "sqlgen"
    out_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Build per-file Phase 1 data lookup ──────────────────────────────────
    # phase1 may contain findings for multiple files; match by source type
    # The structure is either:
    #   phase1["files"][n]  (multi-file) or
    #   phase1             (single-file legacy format)
    phase1_files = _extract_phase1_files(phase1)

    # ── 1. Config SQL ────────────────────────────────────────────────────────
    # Build filename-keyed dict for config_sql.generate (which calls files.items())
    config_files_dict: dict[str, dict] = {}
    for fp in params.files:
        p1 = _find_phase1_file(phase1_files, fp.source)
        if not p1:
            warnings.append(f"No Phase 1 data found for source '{fp.source}' — skipping")
            continue
        filename = p1.get("filename") or p1.get("file_name") or f"{fp.source}.txt"
        config_files_dict[filename] = _build_config_file_input(fp, p1, params.client_id, params.client_name, phase1)

    config_sql_text = config_sql.generate(
        client=params.client_name,
        client_id=params.client_id,
        round_=phase1.get("round", 1),
        files=config_files_dict,
        phase1=phase1,
    )
    config_sql_path = out_dir / f"{params.client_name}_config.sql"
    config_sql_path.write_text(config_sql_text, encoding="utf-8")

    # ── 2. Load sprocs ───────────────────────────────────────────────────────
    sproc_paths: list[str] = []
    sproc_file_list: list[dict] = []

    for fp in params.files:
        p1 = _find_phase1_file(phase1_files, fp.source)
        if not p1:
            continue

        # column_transforms may live at top-level phase1 (Franciscan format) or in the file entry
        col_transforms = p1.get("column_transforms") or phase1.get("column_transforms", [])
        uncovered = p1.get("uncovered_staging", {}).get("required", [])

        # Warn if there are uncovered required staging columns
        if uncovered:
            warnings.append(
                f"[{fp.source}] {len(uncovered)} required staging column(s) have no raw mapping: "
                + ", ".join(uncovered)
                + ". TODO placeholders added to load sproc."
            )

        sproc_text = load_sproc.generate(
            source=fp.source,
            ds_number=fp.ds_number,
            column_transforms=col_transforms,
            uncovered_required=uncovered,
        )

        entity = SOURCE_TO_ENTITY_NAME[fp.source]
        nn = f"{fp.ds_number:02d}"
        sproc_filename = f"cst.dl{nn}_{entity}.sql"
        sproc_path = out_dir / sproc_filename
        sproc_path.write_text(sproc_text, encoding="utf-8")
        sproc_paths.append(str(sproc_path))
        sproc_file_list.append({"source": fp.source, "ds_number": fp.ds_number})

    # ── 3. Liquibase XML ─────────────────────────────────────────────────────
    liq_text = liquibase_xml.generate(
        client_id=params.client_id,
        client_name=params.client_name,
        sproc_files=sproc_file_list,
    )
    liq_path = out_dir / f"{params.client_name}_liquibase.xml"
    liq_path.write_text(liq_text, encoding="utf-8")

    # ── 4. Summary JSON ──────────────────────────────────────────────────────
    summary = {
        "generated_at": generated_at,
        "client_id": params.client_id,
        "client_name": params.client_name,
        "config_sql": str(config_sql_path),
        "sproc_files": [str(p) for p in sproc_paths],
        "liquibase_xml": str(liq_path),
        "warnings": warnings,
    }
    summary_path = out_dir / "generation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return GenerationResult(
        config_sql_path=str(config_sql_path),
        sproc_paths=sproc_paths,
        liquibase_xml_path=str(liq_path),
        summary_path=str(summary_path),
        output_dir=str(out_dir),
        generated_at=generated_at,
        warnings=warnings,
    )


def build_default_params(
    client_id: str,
    client_name: str,
    phase1: dict,
) -> GenerationParams:
    """
    Build GenerationParams with auto-filled defaults from Phase 1 findings.
    The user can override any field before calling generate().
    """
    phase1_files = _extract_phase1_files(phase1)
    raw_database = f"{client_id}_{client_name}_Raw"

    file_params: list[FileParams] = []
    for pf in phase1_files:
        source = pf.get("source") or pf.get("source_type", "")
        if not source:
            continue
        ds_number = SOURCE_TO_DEFAULT_DS_NUMBER.get(source, 1)
        entity = SOURCE_TO_ENTITY_NAME.get(source, source)
        sftp_folder = SFTP_FOLDER_TEMPLATE.format(
            client_id=client_id,
            client_name=client_name,
            ds_number=ds_number,
            entity=entity,
        )
        loaded_folder = LOADED_FOLDER_TEMPLATE.format(
            client_id=client_id,
            client_name=client_name,
            ds_number=ds_number,
            entity=entity,
        )
        # Derive file pattern from actual filename if available
        filename = pf.get("filename") or pf.get("file_name", "")
        if filename:
            base = os.path.splitext(filename)[0]
            file_name_pattern = f"%{base}%"
        else:
            file_name_pattern = f"%{entity}%"

        file_params.append(
            FileParams(
                source=source,
                ds_number=ds_number,
                source_name=entity,
                sftp_folder=sftp_folder,
                loaded_folder=loaded_folder,
                file_name_pattern=file_name_pattern,
            )
        )

    return GenerationParams(
        client_id=client_id,
        client_name=client_name,
        raw_database=raw_database,
        files=file_params,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_phase1_files(phase1: dict) -> list[dict]:
    """Normalise Phase 1 JSON to a list of per-file finding dicts.

    Handles three formats:
      - phase1["files"] is a list of dicts (list format)
      - phase1["files"] is a dict keyed by filename (Franciscan-style)
      - No "files" key — single-file top-level format (legacy)
    """
    files_val = phase1.get("files")
    if isinstance(files_val, list):
        return files_val
    if isinstance(files_val, dict):
        # Dict-keyed-by-filename: inject "filename" key into each entry
        return [{"filename": fname, **data} for fname, data in files_val.items()]
    # No "files" key — single-file top-level format
    return [phase1]


def _find_phase1_file(phase1_files: list[dict], source: str) -> dict | None:
    """Find the Phase 1 file entry matching the given source type."""
    for pf in phase1_files:
        if pf.get("source") == source or pf.get("source_type") == source:
            return pf
    return None


def _build_config_file_input(
    fp: FileParams,
    p1: dict,
    client_id: str,
    client_name: str,
    phase1: dict | None = None,
) -> dict:
    """Build the file input dict expected by config_sql.generate()."""
    phase1 = phase1 or {}
    # column_transforms may live at top-level phase1 (Franciscan) or inside the file entry
    col_transforms = p1.get("column_transforms") or phase1.get("column_transforms", [])
    return {
        "source": fp.source,
        "ds_number": fp.ds_number,
        "source_name": fp.source_name,
        "sftp_folder": fp.sftp_folder,
        "loaded_folder": fp.loaded_folder,
        "file_name_pattern": fp.file_name_pattern,
        "row_terminator": fp.row_terminator,
        "automated_load": fp.automated_load,
        "daily_load": fp.daily_load,
        "column_mappings": p1.get("column_mappings", []),
        "column_transforms": col_transforms,
        "uncovered_required": p1.get("uncovered_staging", {}).get("required", []),
        "delimiter": p1.get("delimiter", ","),
        "col_count": p1.get("col_count", 0),
        "encoding": p1.get("encoding", "UTF-8"),
        "staging_table": p1.get("staging_table", ""),
    }
