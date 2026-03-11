import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter()

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"


def _load_phase5(client: str) -> dict | None:
    p = OUTPUT_DIR / client / "phase5_findings.json"
    if not p.exists():
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


@router.get("/clients")
def list_clients():
    """Return summary cards for every client that has a phase5_findings.json."""
    results = []
    if not OUTPUT_DIR.exists():
        return results

    for client_dir in sorted(OUTPUT_DIR.iterdir()):
        if not client_dir.is_dir():
            continue
        findings = _load_phase5(client_dir.name)
        if findings is None:
            continue
        readiness = findings.get("readiness", {})
        total = readiness.get("total_counts", {})
        results.append({
            "client": client_dir.name,
            "round": findings.get("round", ""),
            "date_run": findings.get("date_run", ""),
            "test_month": findings.get("test_month", ""),
            "readiness": readiness.get("overall", "Unknown"),
            "critical": total.get("CRITICAL", 0),
            "high": total.get("HIGH", 0),
            "medium": total.get("MEDIUM", 0),
            "low": total.get("LOW", 0),
        })
    return results


@router.get("/clients/{client}")
def get_client_findings(client: str):
    """Return full phase5_findings.json for a given client."""
    findings = _load_phase5(client)
    if findings is None:
        raise HTTPException(status_code=404, detail="phase5_findings.json not found")
    return findings


@router.get("/clients/{client}/report")
def download_report(client: str):
    """Serve the most recent Phase 5 Excel report as a file download."""
    client_dir = OUTPUT_DIR / client
    if not client_dir.exists():
        raise HTTPException(status_code=404, detail="Client not found")
    matches = sorted(client_dir.glob("*Phase5*.xlsx"))
    if not matches:
        raise HTTPException(status_code=404, detail="Phase 5 Excel report not found")
    report = matches[-1]
    return FileResponse(
        path=str(report),
        filename=report.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
