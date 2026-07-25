# --------lib we need ------
from pydantic import BaseModel 
from dotenv import load_dotenv
from fastapi import FastAPI
from temporalio.client import Client
import os
import uuid
# ---------
load_dotenv()

TEMPORAL_HOST = os.environ['TEMPORAL_HOST']
TEMPORAL_NAMESPACE = os.environ['TEMPORAL_NAMESPACE']
TEMPORAL_PDF_PROCESS_TASK_QUEUE = os.environ['TEMPORAL_PDF_PROCESS_TASK_QUEUE']


app = FastAPI(
    title="PDF Extraction Client",
    description = "Submits PDF processing jobs to Temporal and returns the result. ",
    version = "1.0"
)

class ProcessPDFRequest(BaseModel):
    s3_path: str

class ProcessPDFStartResponse(BaseModel):
    workflow_id : str
class ProcessPDFExecuteResponse(BaseModel):
    workflow_id : str
    results: dict
async def get_temporal_client() -> Client:
    return await Client.connect(
        TEMPORAL_HOST,
        namespace=TEMPORAL_NAMESPACE,
    )


# Routes 

@app.get('/health')
async def health():
    return{'status':'200 ok'}


@app.post("/process-pdf/execute",response_model=ProcessPDFExecuteResponse)
async def process_pdf(request:ProcessPDFRequest):
    
    workflow_id = f"pdf-pipeline-{uuid.uuid4()}"
    
    client = await get_temporal_client()

    results = await client.execute_workflow(
        "PDFPipelineWorkflow",

        args=[
            {
                's3_path':request.s3_path
            }
        ],

        id = workflow_id,
        task_queue = TEMPORAL_PDF_PROCESS_TASK_QUEUE,
        result_type = dict

    )

    return ProcessPDFExecuteResponse(

        workflow_id=workflow_id,
        results=results

    )


@app.post("/process-pdf/start",response_model=ProcessPDFStartResponse)
async def process_pdf(request:ProcessPDFRequest):
    
    workflow_id = f"pdf-pipeline-{uuid.uuid4()}"
    
    client = await get_temporal_client()

    results = await client.start_workflow(
        "PDFPipelineWorkflow",

        args=[
            {
                's3_path':request.s3_path
            }
        ],

        id = workflow_id,
        task_queue = TEMPORAL_PDF_PROCESS_TASK_QUEUE,
        result_type = dict

    )

    return ProcessPDFStartResponse(

        workflow_id=workflow_id,
        results_type=dict

    )

@app.get("/process-pdf/status/{workflow_id}")
async def get_workflow_status(workflow_id):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id) # connaction with work flow
    
    desc = await handle.describe() # It has the specifications of the current Workflow

    try:
        result = await handle.result()

    except:
        result=None

    workflow_status = desc.status

    return{

        "workflow_id": workflow_id,
        "workflow_status": workflow_status.name.capitalize,
        "workflow_result": result
    }
