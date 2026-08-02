import os
import math
from pathlib import Path
import tempfile
from dataclasses import dataclass

import boto3
import fitz  #pyMuPDF  --page-by-page extraction

import pymupdf4llm
from dotenv import load_dotenv
from openai import OpenAI
from temporalio import activity

load_dotenv()
# __________________ data class
@dataclass
class ExtractPDFInput:
    s3_path : str
    batch_size : int = 2 


@dataclass
class ExtractPDFOutput:
    s3_path : str
    markdown_text : str
    page_count : int


@dataclass 
class CallLLMInput:
    prompt : str

@dataclass 
class CallLLMOutput:
    content : str

# -------- heleprs-------------------

def get_s3_path():
    return boto3.client(
        's3',
        region_name = os.environ['AWS_REGION'],
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
        endpoint_url = os.environ['AWS_S3_ENDPOINT_URL']
    )


def parse_s3_path(s3_path:str):
    s3_path_no_scheme = s3_path.replace('s3://','')
    bucket, _, key = s3_path_no_scheme.partition('/')
    return bucket,key
# activitiy 1 : Extract PDF from s3 <<< input s3 path  ---> output : path content   >>> 

@activity.defn
async def extract_pdf(params: ExtractPDFInput) -> ExtractPDFOutput:

    activity.logger.info(f"Starting ectraction:{ params.s3_path}")
    # heard beat 

    activity.heartbeat({
        "stage": "downloading",
        "s3_path" : params.s3_path,
        "pages_done" : 0,
        "chars_extacted" : 0
    })

    # connection with s3 
    s3_client = get_s3_path()
    bucket,key = parse_s3_path(params.s3_path)

    activity.logger.info(f"Downloading {params.s3_path} from bucket:{bucket} key:{key}")

    # downloda file 
    filename = Path(key).name 
    TEMP_DIR = os.environ['TEMP_DIR']

    local_path = str(Path(TEMP_DIR)/filename)



    s3_client.download_file(
        bucket,
        key,
        local_path
    )

    # count the page of file 
    doc =fitz.open(local_path)
    total_pages = doc.page_count

    activity.logger.info(f'Download {total_pages} page PDF {params.s3_path}')

    all_text_chunks = []
    total_chars_num = 0
    num_batches = math.ceil(total_pages / params.batch_size) 

    for batch_idx in range(num_batches):
        start_page = batch_idx * params.batch_size
        end_page = min(start_page + params.batch_size, total_pages )

        batch_md = pymupdf4llm.to_markdown(
            local_path,
            pages = list(range(start_page,end_page))
        )

        all_text_chunks.append(batch_md)
        total_chars_num+=len(batch_md)
        activity.heartbeat({
            "stage":        "extracting",
            "s3_path":       params.s3_path,
            "paged_done":    end_page,
            "total_pages":   total_pages,
            "batch":         f'{start_page + 1}-{end_page}',
            "chars_extracted":total_chars_num,
            "progress_pct": round(end_page / total_pages *100),

        })


    full_md = "\n\n".join(all_text_chunks)


    activity.heartbeat({
                "stage":        "done",
                "s3_path":       params.s3_path,
                "paged_done":    total_pages,
                "chars_extracted": total_chars_num

                })
    return ExtractPDFOutput(
        s3_path=params.s3_path,
        markdown_text=full_md,
        page_count=total_pages
    )

# activitiy 2 : Call the llm <<< input --> prompet and content -- output ---> reustl >>>
@activity.defn
async def call_llm(params: CallLLMInput)->CallLLMOutput:
    activity.logger.info("Calling LLM")
    activity.heartbeat({
        "stage":"calling_llm",
        "prompt_chats" : len(params.prompt)

    })

    llm_client = OpenAI(
        api_key = os.environ["API_KEY"],
        base_url = "https://inference.dahl.global/v1"
    )

    response = llm_client.chat.completions.create(
        model = os.environ.get('MODEL_NAME'),
        messages = [{"role":"user","content":params.prompt}],
        max_tokens = 8000,
    )

    content = response.choices[0].message.content

    activity.logger.info(f'llm returned {len(content)} chars')
    
    return CallLLMOutput(
        content=content
    )