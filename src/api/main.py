from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
import subprocess
import yaml
import os
from pathlib import Path

from .schemas import (
    InferSchemaRequest, InferSchemaResponse,
    RunPipelineRequest, RunPipelineResponse
)

app = FastAPI(title="FlinkForge API", version="3.0")

@app.post("/infer-schema", response_model=InferSchemaResponse)
async def infer_schema(req: InferSchemaRequest):
    cmd = ["python", "/workspace/infer_schema.py", req.file_path]
    if req.table_name:
        cmd.append(req.table_name)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise HTTPException(500, detail=result.stderr)

    generated_name = req.table_name or Path(req.file_path).stem
    yaml_path = f"/workspace/config/sources/{generated_name}.yaml"
    if not Path(yaml_path).exists():
        raise HTTPException(500, "YAML not generated")

    return InferSchemaResponse(
        yaml_content=Path(yaml_path).read_text(),
        table_name=generated_name
    )

@app.post("/run-pipeline", response_model=RunPipelineResponse)
async def run_pipeline(req: RunPipelineRequest):
    if not Path(req.pipeline_path).exists():
        raise HTTPException(404, f"Pipeline not found: {req.pipeline_path}")

    cmd = ["python", "/workspace/run_pipeline.py", req.pipeline_path]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise HTTPException(500, detail=result.stderr)

    # Parse our own log for row counts (you can improve this later)
    stdout = result.stdout
    rows_read = int([l for l in stdout.splitlines() if "Read" in l][-1].split()[-1]) if "Read" in stdout else 0
    rows_written = int([l for l in stdout.splitlines() if "Wrote" in l][-1].split()[-1]) if "Wrote" in stdout else 0

    return RunPipelineResponse(
        status="success",
        message="Pipeline completed",
        rows_read=rows_read,
        rows_written=rows_written
    )

@app.get("/health")
async def health():
    return {"status": "ALIVE"}