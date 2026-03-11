import asyncio
import os
import queue
import subprocess
import sys
import threading
import uuid
from pathlib import Path
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# In-memory job registry  {job_id: {"proc": Popen, "queue": Queue, "output": [lines], "done": bool}}
_jobs: dict[str, dict] = {}


class RunRequest(BaseModel):
    client: str
    round: str
    date_start: str = ""
    date_end: str = ""


def _reader_thread(proc: subprocess.Popen, q: queue.Queue) -> None:
    """Background daemon thread: drains proc.stdout into the queue via explicit readline().

    Using readline() rather than iterating proc.stdout directly avoids Windows
    text-mode pipe buffering quirks.  Sends ('line', text) for each output line,
    then ('done', returncode) when the process exits.
    """
    try:
        while True:
            line = proc.stdout.readline()
            if line:
                q.put(("line", line.rstrip("\n")))
            else:
                # Empty readline() == EOF — process finished writing
                break
    finally:
        proc.wait()
        q.put(("done", proc.returncode))


@router.post("/run")
def start_run(req: RunRequest):
    """Launch run_all.py as a subprocess and return a job_id."""
    if not req.client.strip():
        raise HTTPException(status_code=422, detail="client is required")
    if not req.round.strip():
        raise HTTPException(status_code=422, detail="round is required")

    cmd = [
        sys.executable,
        "-u",  # force unbuffered stdout/stderr in the child process
        str(PROJECT_ROOT / "run_all.py"),
        req.client,
        req.round,
        "--no-prompt",
    ]
    if req.date_start:
        cmd += ["--date-start", req.date_start]
    if req.date_end:
        cmd += ["--date-end", req.date_end]

    # PYTHONUNBUFFERED=1 ensures every print() in the child flushes immediately
    child_env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}

    # CREATE_NO_WINDOW prevents the subprocess from inheriting uvicorn's console
    # handles on Windows, which can interfere with pipe reading.
    flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(PROJECT_ROOT),
        env=child_env,
        creationflags=flags,
    )

    q: queue.Queue = queue.Queue()
    t = threading.Thread(target=_reader_thread, args=(proc, q), daemon=True)
    t.start()

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"proc": proc, "queue": q, "output": [], "done": False}
    return {"job_id": job_id}


async def _stream_output(job_id: str) -> AsyncGenerator[str, None]:
    job = _jobs.get(job_id)
    if job is None:
        yield "data: ERROR: job not found\n\n"
        return

    # SSE comment — establishes the connection immediately so the browser
    # doesn't fire onerror while waiting for the first real data line.
    yield ": connected\n\n"

    q: queue.Queue = job["queue"]

    # Pure asyncio polling: q.get_nowait() is non-blocking; asyncio.sleep()
    # yields control back to the event loop between polls.  This avoids
    # run_in_executor entirely — no Future/CancelledError races on Windows.
    POLL_INTERVAL = 0.05        # seconds between queue polls (50 ms)
    KEEPALIVE_INTERVAL = 15.0   # seconds between SSE keepalive comments

    loop = asyncio.get_event_loop()
    last_keepalive = loop.time()

    while True:
        try:
            kind, value = q.get_nowait()
        except queue.Empty:
            # Nothing in the queue yet — yield to the event loop and try again
            now = loop.time()
            if now - last_keepalive >= KEEPALIVE_INTERVAL:
                yield ": keepalive\n\n"
                last_keepalive = now
            await asyncio.sleep(POLL_INTERVAL)
            continue
        except Exception as exc:
            job["done"] = True
            yield f"data: ERROR: {exc}\n\n"
            yield "data: __DONE__ exit=1\n\n"
            break

        if kind == "line":
            job["output"].append(value)
            yield f"data: {value}\n\n"
        elif kind == "done":
            job["done"] = True
            exit_code = value
            yield f"data: __DONE__ exit={exit_code}\n\n"
            break


@router.get("/run/{job_id}/progress")
def stream_progress(job_id: str):
    """SSE endpoint — streams subprocess stdout line by line."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="job not found")
    return StreamingResponse(
        _stream_output(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/run/{job_id}/status")
def job_status(job_id: str):
    """Return whether the job is complete and its buffered output."""
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "done": job["done"],
        "exit_code": job["proc"].returncode,
        "lines": job["output"],
    }
