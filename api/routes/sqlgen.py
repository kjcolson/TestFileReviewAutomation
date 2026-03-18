"""
api/routes/sqlgen.py

POST /api/sqlgen/generate  — runs SQL generation for a client.
GET  /api/sqlgen/defaults/{client}  — returns auto-filled parameter defaults.
GET  /api/sqlgen/download/{client}/{filename}  — downloads a generated file.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel

from sqlgen.generator import FileParams, GenerationParams, GenerationResult, build_default_params, generate

router = APIRouter()

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────────────────────────────────────

class FileParamsModel(BaseModel):
    source: str
    ds_number: int
    source_name: str
    sftp_folder: str
    loaded_folder: str
    file_name_pattern: str
    row_terminator: str = "0x0a"
    automated_load: bool = False
    daily_load: bool = False


class GenerateRequest(BaseModel):
    client: str
    client_id: str
    client_name: str
    raw_database: str
    files: list[FileParamsModel]


class GenerateResponse(BaseModel):
    config_sql_path: str
    sproc_paths: list[str]
    liquibase_xml_path: str
    summary_path: str
    output_dir: str
    generated_at: str
    warnings: list[str]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _phase1_path(client: str) -> Path:
    return OUTPUT_DIR / client / "phase1_findings.json"


def _sqlgen_dir(client: str) -> Path:
    return OUTPUT_DIR / client / "sqlgen"


def _load_phase1(client: str) -> dict:
    p = _phase1_path(client)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"phase1_findings.json not found for client '{client}'")
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/sqlgen/defaults/{client}")
def get_defaults(client: str):
    """
    Return auto-filled GenerationParams defaults from Phase 1 findings.
    The UI pre-fills the form with these values; the user may edit before generating.
    """
    phase1 = _load_phase1(client)
    # Use the client folder name as a placeholder ID — user must supply the real 4-digit ID
    defaults = build_default_params(
        client_id="0000",  # placeholder; UI will override
        client_name=phase1.get("client", client),
        phase1=phase1,
    )
    return {
        "client_name": defaults.client_name,
        "raw_database": defaults.raw_database,
        "files": [
            {
                "source": f.source,
                "ds_number": f.ds_number,
                "source_name": f.source_name,
                "sftp_folder": f.sftp_folder,
                "loaded_folder": f.loaded_folder,
                "file_name_pattern": f.file_name_pattern,
                "row_terminator": f.row_terminator,
                "automated_load": f.automated_load,
                "daily_load": f.daily_load,
            }
            for f in defaults.files
        ],
    }


@router.post("/sqlgen/generate", response_model=GenerateResponse)
def generate_sql(req: GenerateRequest):
    """
    Generate config SQL, load sproc(s), and Liquibase XML for a client.
    Writes files to output/{client}/sqlgen/ and returns paths.
    """
    phase1_p = _phase1_path(req.client)
    if not phase1_p.exists():
        raise HTTPException(status_code=404, detail=f"phase1_findings.json not found for client '{req.client}'")

    params = GenerationParams(
        client_id=req.client_id,
        client_name=req.client_name,
        raw_database=req.raw_database,
        files=[
            FileParams(
                source=f.source,
                ds_number=f.ds_number,
                source_name=f.source_name,
                sftp_folder=f.sftp_folder,
                loaded_folder=f.loaded_folder,
                file_name_pattern=f.file_name_pattern,
                row_terminator=f.row_terminator,
                automated_load=f.automated_load,
                daily_load=f.daily_load,
            )
            for f in req.files
        ],
        output_dir=str(_sqlgen_dir(req.client)),
    )

    try:
        result: GenerationResult = generate(params, str(phase1_p))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return GenerateResponse(
        config_sql_path=result.config_sql_path,
        sproc_paths=result.sproc_paths,
        liquibase_xml_path=result.liquibase_xml_path,
        summary_path=result.summary_path,
        output_dir=result.output_dir,
        generated_at=result.generated_at,
        warnings=result.warnings,
    )


@router.get("/sqlgen/download/{client}/{filename}")
def download_file(client: str, filename: str):
    """Download a generated file from output/{client}/sqlgen/{filename}."""
    # Prevent path traversal
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    file_path = _sqlgen_dir(client) / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found. Run generation first.")

    media_type = "application/xml" if filename.endswith(".xml") else "text/plain"
    return FileResponse(str(file_path), filename=filename, media_type=media_type)


@router.get("/sqlgen/preview/{client}/{filename}", response_class=PlainTextResponse)
def preview_file(client: str, filename: str):
    """Return the text content of a generated file for in-browser preview."""
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    file_path = _sqlgen_dir(client) / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found. Run generation first.")

    return file_path.read_text(encoding="utf-8")
