"""Heimdall — the API gateway (interim FastAPI build; Node rewrite lands
with auth in step 3 of PLAN.md).

Responsibilities today:
  * intake: accept a register upload, stage files in STORAGE_DIR/uploads
    (the local stand-in for the private S3 bucket), create the QUEUED
    record + SEAL job, then wait for thor and return the full result —
    a synchronous facade over the async pipeline, so the existing web
    UI needs no changes yet.
  * serve release masters staged by thor (STORAGE_DIR/release).
  * expose record/job status (the wizard's future polling endpoint).
  * reverse-proxy every verification/lab path to odin.
"""

import asyncio
import io
import json
import os
import uuid
import zipfile
from pathlib import Path

import boto3
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

from . import pg

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

ODIN_URL = os.environ.get("ODIN_URL", "http://127.0.0.1:8001")
STORAGE_DIR = Path(os.environ.get(
    "STORAGE_DIR",
    Path(__file__).resolve().parent.parent.parent / "_localstore"))
UPLOADS = STORAGE_DIR / "uploads"
RELEASE = STORAGE_DIR / "release"
SEAL_TIMEOUT_S = int(os.environ.get("SEAL_TIMEOUT_S", "900"))

# SQS is the delivery nudge; the jobs table is the truth. A failed send
# must never fail intake — thor's fallback DB poll picks the job up.
SQS_URL = os.environ.get("SQS_SEAL_QUEUE_URL")
_sqs = (boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "ap-south-1"))
        if SQS_URL else None)


def _nudge_thor(record_id: str) -> None:
    if _sqs is None:
        return
    try:
        _sqs.send_message(QueueUrl=SQS_URL,
                          MessageBody=json.dumps({"kind": "SEAL",
                                                  "record_id": record_id}))
    except Exception as e:
        print(f"[heimdall] sqs nudge failed (thor will poll): {e}", flush=True)

app = FastAPI(title="MotherTape Heimdall (gateway)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _project_zip_from_upload(
    project: UploadFile | None,
    project_files: list[UploadFile] | None,
    project_paths: list[str] | None,
) -> bytes:
    """Accept a .logicx either as a zip file or as folder-picked files+paths."""
    if project is not None:
        return await project.read()
    if project_files and project_paths:
        if len(project_files) != len(project_paths):
            raise HTTPException(status_code=422, detail="project files/paths must pair up")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f, rel in zip(project_files, project_paths):
                rel = rel.replace("\\", "/").lstrip("/")
                if not rel or ".." in rel.split("/"):
                    raise HTTPException(status_code=422, detail=f"unsafe path: {rel}")
                zf.writestr(rel, await f.read())
        return buf.getvalue()
    raise HTTPException(status_code=422,
                        detail="provide the .logicx as a zip or as folder files+paths")


@app.post("/product/register")
async def product_register(
    artist: str = Form(...),
    master: UploadFile = File(...),
    stems: list[UploadFile] = File(...),
    project: UploadFile | None = File(None),
    project_files: list[UploadFile] | None = File(None),
    project_paths: list[str] | None = Form(None),
):
    """Stage inputs, enqueue a SEAL job, wait for thor, return the result."""
    if len(stems) < 2:
        raise HTTPException(status_code=422, detail="upload at least two stems")
    project_zip = await _project_zip_from_upload(project, project_files, project_paths)

    record_id = str(uuid.uuid4())
    updir = UPLOADS / record_id
    updir.mkdir(parents=True, exist_ok=True)

    master_name = Path(master.filename or "master.wav").name
    master_path = updir / f"master_{master_name}"
    master_path.write_bytes(await master.read())
    stem_paths = []
    for i, s in enumerate(stems):
        p = updir / f"stem_{i}_{Path(s.filename or 'stem.wav').name}"
        p.write_bytes(await s.read())
        stem_paths.append(str(p))
    project_path = updir / "project.zip"
    project_path.write_bytes(project_zip)

    pg.create_record_and_job(record_id, artist, {
        "artist": artist,
        "master_path": str(master_path),
        "master_name": master_name,
        "stem_paths": stem_paths,
        "project_zip_path": str(project_path),
    })
    _nudge_thor(record_id)

    # Synchronous facade: wait for thor. The wizard's polling upgrade
    # (GET /records/{id}) replaces this wait in a later step.
    deadline = asyncio.get_event_loop().time() + SEAL_TIMEOUT_S
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(1.0)
        job = pg.job_for_record(record_id)
        if job is None:
            continue
        if job["status"] == "SUCCEEDED":
            return job["result"]
        if job["status"] == "FAILED":
            raise HTTPException(status_code=422, detail=job["error"])
    raise HTTPException(status_code=504, detail="sealing timed out")


@app.get("/records/{record_id}")
def record_status(record_id: str):
    """Job status for a record — the future wizard polling endpoint."""
    try:
        job = pg.job_for_record(record_id)
    except Exception:
        raise HTTPException(status_code=422, detail="bad record id")
    if job is None:
        raise HTTPException(status_code=404, detail="no such record")
    return {"record_id": record_id, "status": job["status"],
            "error": job["error"], "result": job["result"]}


@app.get("/records/{record_id}/release")
def record_release(record_id: str):
    path = RELEASE / f"{record_id}.wav"
    if not path.exists():
        raise HTTPException(status_code=404, detail="no release file")
    return FileResponse(path, media_type="audio/wav",
                        filename=f"{record_id}_watermarked_signed.wav")


# ---------------------------------------------------------------------------
# everything else proxies to odin (link, manifest, checkers, labs, simulate)
# ---------------------------------------------------------------------------

_HOP = {"host", "content-length", "connection", "keep-alive", "expect",
        "transfer-encoding", "upgrade", "proxy-connection"}

_client = httpx.AsyncClient(base_url=ODIN_URL, timeout=httpx.Timeout(600.0))


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def odin_proxy(path: str, request: Request):
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP}
    req = _client.build_request(
        request.method, f"/{path}",
        params=request.query_params,
        headers=headers,
        content=request.stream())
    try:
        resp = await _client.send(req, stream=True)
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="odin is unreachable")
    out_headers = {k: v for k, v in resp.headers.items()
                   if k.lower() not in ("content-encoding", "content-length",
                                        "transfer-encoding", "connection")}
    return StreamingResponse(resp.aiter_bytes(), status_code=resp.status_code,
                             headers=out_headers,
                             background=None)
