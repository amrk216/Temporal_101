


from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

from temporalio.common import RetryPolicy



with workflow.unsafe.imports_passed_through():
    from activities import (
        download_s3_file,
        extract_to_markdown,
        upload_markdown,
    )
    from helpers import (
        DownloadInput,
        ExtractInput,
        UploadInput,
        DownloadOutput,
        )
    

@dataclass
class PDFPipelineInput:
    s3_path: str

@dataclass
class PDFPipelineOutput:
    original_s3_path: str



DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0, #double the wait each retry. ex:2s , 4s , 8s
    maximum_interval=timedelta(seconds=60),
    maximum_attempts= 5

)


@workflow.defn
class PDFPipelineWorkflow:

    @workflow.run
    async def run(self, params: PDFPipelineInput) -> PDFPipelineOutput:
        workflow.logger.info(f'Starting PDF pipline for: {params.s3_path} ')

        #step 1 Download PDF from s3

        download_result = await workflow.execute_activity(
            download_s3_file,
            DownloadInput(s3_path=params.s3_path),
            retry_policy=DEFAULT_RETRY,
            start_to_close_timeout=timedelta(minutes=3 )
        )

        #step 2 Etract PDF to markdown

        extract_result = await workflow.execute_activity(
            extract_to_markdown,
            ExtractInput(local_pdf_path=download_result.local_path),
            retry_policy=DEFAULT_RETRY,
            start_to_close_timeout=timedelta(minutes=10)
            
        )
        #step 3 Upload Markdown to s3
        upload_result = await workflow.execute_activity(
            upload_markdown,
            UploadInput(markdown_text=extract_result.markdown_text,
                        original_s3_path=params.s3_path),
                        retry_policy=DEFAULT_RETRY,
                        start_to_close_timeout=timedelta(minutes=3)

        )
        workflow.logger.info(f'Pipeline complete, Output: {upload_result.original_s3_path} ')

        return PDFPipelineOutput(
            original_s3_path= upload_result.original_s3_path
        )
