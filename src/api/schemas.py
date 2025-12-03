from pydantic import BaseModel
from typing import List, Any

class InferSchemaRequest(BaseModel):
    file_path: str
    table_name: str

class InferSchemaResponse(BaseModel):
    yaml_content: str
    table_name: str

class RunPipelineRequest(BaseModel):
    pipeline_file: str

class RunPipelineResponse(BaseModel):
    status: str
    message: str
    rows_read: int = 0
    rows_written: int = 0 

class GetPipelineStatusRequest(BaseModel):
    pipeline_id: str

class GetPipelineStatusResponse(BaseModel):
    status: str