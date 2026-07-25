import os
import sys
import tempfile
import logging
from dotenv import load_dotenv
 
from dataclasses import dataclass

from temporalio import activity
import boto3
import pymupdf4llm 
from pathlib import Path
from helpers import (parse_s3_path, get_S3_client,TEMP_DIR,
                     DownloadInput, DownloadOutput, ExtractInput,
                      ExtractOutput, UploadInput, UploadOutput)

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# this func does We will merge the two that it is one path and you choose what you need (S3 path ex: name_bucket/path)

async def parse_s3_path(s3_path: str):
    s3_path_no_scheme = s3_path.replace("s3://","")
    bucket, _, key=s3_path_no_scheme.partition('/') # ex: (temporal-dev)---> bucket / (folder1) ---> idont care about this /(file.pdf)--> key 

    return bucket, key

# this func to Download a pdf from s3 . Returns local file path. 
@activity.defn
async def download_s3_file(param:DownloadInput) -> DownloadOutput:
    
    bucket,key = await parse_s3_path(param.s3_path)

    filename = Path(key).name

    local_path = str(Path(TEMP_DIR) / filename )# path of the file save
    
    logger.info(f"Downloading: s3://{bucket}/{key} => {local_path}")

    #to download file
    s3_client = get_S3_client()
    s3_client.download_file(
        bucket,
        key,
        local_path
    )

    logger.info(f'Compeleted Downloading: {local_path}')

    return DownloadOutput(local_path=local_path)


# okay we downloeded file , the next is step in extracted content from file 

@activity.defn
async def extract_to_markdown (param: ExtractInput) -> ExtractOutput:
    logger.info(f"Extaction complete {param.local_pdf_path}")

    try:
        markdown_text = pymupdf4llm.to_markdown(param.local_pdf_path)
        logger.info(f"Extraction complet {len(markdown_text)} characters")
        return ExtractOutput(markdown_text=markdown_text)
    
    except Exception as e: 
        logger.error(f"check the locla_pdf_path {param.local_pdf_path} : {e}")

        raise



# upload the markdown file to S3
@activity.defn
async def upload_markdown(params: UploadInput) -> UploadOutput:
    bucket, key = await parse_s3_path(params.original_s3_path)
    md_key = key.replace(".pdf", ".md")

    logger.info(f"uploading markdown -> s3://{bucket}/{md_key}")
    s3_client = get_S3_client()
   
    s3_client.put_object(
        Bucket = bucket,
        Key = md_key,  

        Body = params.markdown_text.encode("utf-8"),
        ContentType = "text/markdown"   
    )
    output_path = f"s3://{bucket}/{md_key}"
    logger.info(f'Upload Complete: {output_path}')
    return UploadOutput(original_s3_path=output_path)
