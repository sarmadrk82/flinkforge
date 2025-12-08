from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import subprocess
import yaml
from pathlib import Path
from typing import Dict

app = FastAPI(title="FlinkForge V3 API", version="3.0")
CONFIG_DIR = Path("/workspace/config")

class PipelineRun(BaseModel):
    pipeline_name: str

pipeline_status = {}  # In-memory store: {name: {'status': 'running', 'rows': 0, 'job_id': str}}

def run_pipeline_bg(pipeline_name: str):
    """Background runner — calls your existing run_pipeline.py"""
    pipeline_status[pipeline_name] = {'status': 'running', 'rows': 0, 'job_id': 'flink-job-123'}
    try:
        result = subprocess.run(
            ['python', '/workspace/run_pipeline.py', str(CONFIG_DIR / 'pipelines' / f'{pipeline_name}.yaml')],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            pipeline_status[pipeline_name]['status'] = 'success'
            pipeline_status[pipeline_name]['rows'] = 4  # Parse from logs or query sink — TODO: enhance
        else:
            pipeline_status[pipeline_name]['status'] = 'failed'
            pipeline_status[pipeline_name]['error'] = result.stderr
    except Exception as e:
        pipeline_status[pipeline_name]['status'] = 'error'
        pipeline_status[pipeline_name]['error'] = str(e)

@app.post("/pipelines/{name}/run")
async def trigger_pipeline(name: str, bg_tasks: BackgroundTasks):
    if not (CONFIG_DIR / 'pipelines' / f'{name}.yaml').exists():
        raise HTTPException(404, f"Pipeline '{name}' not found")
    bg_tasks.add_task(run_pipeline_bg, name)
    return {"message": f"Pipeline '{name}' triggered", "job_id": pipeline_status.get(name, {}).get('job_id')}

@app.get("/pipelines/{name}/status")
async def get_status(name: str) -> Dict:
    status = pipeline_status.get(name, {'status': 'not_started'})
    # Bonus: Query Postgres row count live (using psycopg2 or similar — add import if needed)
    # cur.execute("SELECT COUNT(*) FROM customers"); status['rows'] = cur.fetchone()[0]
    return status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)