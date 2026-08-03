import json
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

        self._review_decision: Optional[str] = None
        self._review_feedback: str = ""
        self._approved_by: str = ""


    # Query : status of the workflow
    @workflow.query
    def get_status(self) -> dict:
        return {
            "status": self._status,
            "pdfs_processed": len(self._summaries),
            "report_ready": json.dumps(self._report,ensure_ascii=False)[:500],
            "approved_by": self._approved_by,
        }

    # Query 
    @workflow.query
    def get_report(self) -> dict:
        return {
            "status": self._status,
            "report": self._report,
            "approved_by": self._approved_by,
            "source": [s["s3_path"] for s in self._summaries]
        }

        # signal handler for review decision
    @workflow.signal
    async def assign_reviewer(self,name:str):
        self._approved_by = name

    @workflow.update
    async def submit_decision(self, decision:str, feedback:str = "") -> str:
        self._review_decision = decision
        self._review_feedback = feedback

        return f"Decision '{decision}' recorded "

    @submit_decision.validator
    def validate_decision(self, decision:str, feedback:str = "")->None:
        if decision not in ("approve", "revise"):
            raise ValueError(f"Must be 'approve' or 'revise', got '{decision}'")

        if decision == "revise" and not feedback.strip():
            raise ValueError(f"Feedback is required when requesting revision")


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
            for i, summary in enumerate(self._summaries)
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


        for revision_no in range(params.max_revisions+1):

            self._status = "awaiting_review"
            workflow.logger.info(f"Awaiting review for human review {revision_no}")

            self._review_decision = None


            try: 
                await workflow.wait_condition(
                    lambda: self._review_decision is not None,
                    timeout=timedelta(days=3),
                )
            except asyncio.TimeoutError:
                workflow.logger.warning("Review timed out after 3 days, proceeding with current report")
                break

            if self._review_decision == "approve":
                workflow.logger.info("Report approved by reviewer")
                self._approved_by = workflow.info().workflow_id
                break

            self._status = "revising"
            workflow.logger.info(f"Revision requested by reviewer: {self._review_feedback}")

            llm_prompt = _REVISION_PROMPT.format(
                report = json.dumps(self._report, indent=2,ensure_ascii=False),
                feedback = self._review_feedback
            )


            revised_report = await workflow.execute_activity(
                call_llm,
                CallLLMInput(prompt=llm_prompt),
                    retry_policy=DEFAULT_RETRY_POLICY,
                    start_to_close_timeout=timedelta(minutes=3),
                    heartbeat_timeout=timedelta(seconds=180)
            )

            self._report = json_repair.loads(revised_report.content)
            # Revicsion completed, loop back to await review again
            self._status = "completed"
        return ContractReviewOutput(
                report = self._report,
                source = [s["s3_path"] for s in self._summaries],
                approved_by = self._approved_by
            )


             