# the worker is the process thatr run your code
# it connedts to temporal , polls for tasks
# and executes workflows and activities.

import os
import logging
import asyncio


from dotenv import load_dotenv
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from workflow_process_pdf import PDFPipelineWorkflow
load_dotenv()



TEMPORAL_HOST = os.environ['TEMPORAL_HOST']
TEMPORAL_NAMESPACE = os .environ['TEMPORAL_NAMESPACE']
TEMPORAL_PDF_PROCESS_TASK_QUEUE = os.environ['TEMPORAL_PDF_PROCESS_TASK_QUEUE']

with workflow.unsafe.imports_passed_through():
    from activities import (
        download_s3_file,
        extract_to_markdown,
        upload_markdown,
    )


    async def main():
        temporal_client = await Client.connect(
            TEMPORAL_HOST,
            namespace = TEMPORAL_NAMESPACE,
        )

        worker_pdf_process = Worker(
            temporal_client,
            task_queue = TEMPORAL_PDF_PROCESS_TASK_QUEUE,
            workflows = [PDFPipelineWorkflow],
            activities=[download_s3_file,
                        extract_to_markdown,
                        upload_markdown,]
        )

        print(f'Worker started. poling task queue:{TEMPORAL_PDF_PROCESS_TASK_QUEUE}')
        await worker_pdf_process.run()



if __name__ == "__main__":
    asyncio.run(main())