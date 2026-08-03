import os
import logging
import asyncio


from dotenv import load_dotenv
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from activities import extract_pdf, call_llm
from parent_workflow import ContractReviewWorkflow
from child_workflow import PDFSummaryWorkflow

load_dotenv()



TEMPORAL_HOST = os.environ['TEMPORAL_HOST']
TEMPORAL_NAMESPACE = os .environ['TEMPORAL_NAMESPACE']
TEMPORAL_TASK_QUEUE = os.environ['TEMPORAL_TASK_QUEUE']

async def main():
    temporal_client = await Client.connect(TEMPORAL_HOST, namespace = TEMPORAL_NAMESPACE)


    worker = Worker(
        temporal_client,
        task_queue = TEMPORAL_TASK_QUEUE,
        workflows = [ContractReviewWorkflow, PDFSummaryWorkflow],
        activities=[extract_pdf, call_llm]
    )
    print(f'Worker started. poling task queue:{TEMPORAL_TASK_QUEUE}')
    await worker.run()



if __name__ == "__main__":
    asyncio.run(main())