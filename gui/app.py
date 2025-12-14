import streamlit as st
import requests
import time
import glob
import yaml
from pathlib import Path

st.set_page_config(page_title="FlinkForge V3", layout="wide")
st.title("🚀 FlinkForge V3 — Pipeline Orchestrator")

API_BASE = "http://localhost:8000"  # or your API container host
PIPELINES_DIR = Path("/workspace/config/pipelines")

def load_pipelines():
    pipelines = []
    for yaml_file in PIPELINES_DIR.glob("*.yaml"):
        with open(yaml_file) as f:
            config = yaml.safe_load(f)
        pipelines.append({
            "name": yaml_file.stem,
            "file": str(yaml_file),
            "sources": config.get("sources", []),
            "targets": config.get("targets", []),
            "sql_count": len(config.get("pipeline_sql", [])) if isinstance(config.get("pipeline_sql"), list) else 1
        })
    return sorted(pipelines, key=lambda x: x["name"])

pipelines = load_pipelines()

if not pipelines:
    st.warning("No pipelines found in /workspace/config/pipelines/")
    st.stop()

for pipe in pipelines:
    with st.container():
        col1, col2, col3 = st.columns([3, 1, 2])
        with col1:
            st.subheader(f"📄 {pipe['name']}")
            st.write(f"**Sources:** {', '.join(pipe['sources']) or 'None'}")
            st.write(f"**Targets:** {', '.join(pipe['targets']) or 'None'}")
            st.write(f"**Statements:** {pipe['sql_count']}")
        with col2:
            if st.button(f"RUN →", key=pipe['name'], use_container_width=True):
                with st.spinner(f"Triggering {pipe['name']}..."):
                    try:
                        resp = requests.post(f"{API_BASE}/pipelines/{pipe['name']}/run")
                        if resp.status_code == 200:
                            st.success("Pipeline triggered!")
                            status_placeholder = st.empty()
                            progress_bar = st.progress(0)
                            for i in range(100):
                                time.sleep(0.1)
                                progress_bar.progress(i + 1)
                            status = requests.get(f"{API_BASE}/pipelines/{pipe['name']}/status").json()
                            st.json(status)
                            if status.get("status") == "success":
                                st.balloons()
                        else:
                            st.error(f"API error: {resp.text}")
                    except Exception as e:
                        st.error(f"Failed to trigger: {e}")
        with col3:
            st.write("")  # spacer

st.sidebar.info("FlinkForge V3 — GUI Edition")
st.sidebar.caption("Pipelines auto-discovered from config/pipelines/")