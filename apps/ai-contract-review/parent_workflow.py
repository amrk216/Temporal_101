import textwrap
from dataclasses import dataclass
from datetime import timedelta
import asyncio
from typing import Optional

import json_repair
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy
from prompts import _SYNTHESIS_PROMPT, _REVISION_PROMPT

with workflow.unsafe.imports_passed_through():
    from activities import (
        call_llm,CallLLMInput,
    )
    from child_workflow import(
        PDFSummaryWorkflow,PDFSummayInput
    )


@dataclass
class ContractReviewInput:
    s3_paths:list
    max_revisions:int = 3

@dataclass
class ContractReviewOutput:
    report:str
    source: list
    approved_by: str

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=4
)

@workflow.defn
class ContractReviewWorkflow:
    def __init__(self):
        self._status : str = "processing"
        self._summaries: list = []
        self._report: str= ""

    @workflow.run
    async def run(self, params: ContractReviewInput) -> ContractReviewOutput:
        self._status = "extracting"

        workflow.logger.info(f"Fanning out to {len(params.s3_paths)} child")

        workflow_id = workflow.info().workflow_id
        workflow_task_queue = workflow.info().task_queue

        handels = await asyncio.gather(
            *[
                workflow.start_child_workflow(
                    PDFSummaryWorkflow.run,
                    PDFSummayInput(s3_path = current_s3_path
                                ),
                    id =f"{workflow_id}-pdf-{idx+1}",
                    task_queue = workflow_task_queue,
                    parent_close_policy = ParentClosePolicy.ABANDON,
                )

                for idx, current_s3_path in enumerate(params.s3_paths)
            ]
        )


        row_results = await asyncio.gather(

            *handels,
            return_exceptions=True
            
            )

        for i , res in enumerate(row_results):
                if isinstance(res, Exception):
                    workflow.logger.error(f"PDF {i} faild: {res}")
                else:
                    self._summaries.append({
                        "s3_path": res.s3_path,
                        "summary": res.summary,
                        "key_risks": res.key_risks
                    })
        if len(self._summaries) == 0:
                raise ApplicationError("All child workflows failed")


        # Step 2 : Synthesize the summaries into a single report


        self ._status = "synthesizing"
        workflow.logger.info(f"Summarizing {len(self._summaries)} summaries")

        combined_summaries = "\n\n".join([
            f"** Contract {i+1} ({summary['s3_path']}) Summary **\n{summary['summary']}\n\n**Key Risks**\n{summary['key_risks']}"
            for i, summary in self._summaries
        ])

        llm_prompt = _SYNTHESIS_PROMPT.format(
            summaries = combined_summaries,
            n = len(self._summaries)
        )

        llm_result = await workflow.execute_activity(
            call_llm,
            CallLLMInput(prompt=llm_prompt),
                retry_policy=DEFAULT_RETRY_POLICY,
                start_to_close_timeout=timedelta(minutes=3),
                heartbeat_timeout=timedelta(seconds=180)
        )

        self._report = json_repair.loads(llm_result.content)
            