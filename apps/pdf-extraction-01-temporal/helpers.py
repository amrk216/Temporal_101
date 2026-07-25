from dataclasses import dataclass
import os
import sys
import tempfile
import logging
from dotenv import load_dotenv

import boto3
import pymupdf4llm 
from pathlib import Path

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


load_dotenv()

AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY']
AWS_REGION=os.environ['AWS_REGION']
AWS_S3_ENDPOINT_URL=os.environ['AWS_S3_ENDPOINT_URL']
S3_BUCKET_NAME=os.environ['S3_BUCKET_NAME']
TEMP_DIR=os.environ['TEMP_DIR']
os.makedirs(TEMP_DIR,exist_ok=True)

#data class to store the result of the pdf extraction
@dataclass
class DownloadInput:
    s3_path: str
@dataclass
class DownloadOutput:
    local_path: str 

@dataclass
class ExtractInput:
    local_pdf_path: str
@dataclass
class ExtractOutput:
    markdown_text: str

@dataclass
class UploadInput:
    markdown_text: str
    original_s3_path: str
@dataclass
class UploadOutput:
    original_s3_path: str




# this func to connection between client and S3

def get_S3_client():
    
    return boto3.client(
        "s3",
        region_name = AWS_REGION,
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        endpoint_url = AWS_S3_ENDPOINT_URL,    
                )


# this func does We will merge the two that it is one path and you choose what you need (S3 path ex: name_bucket/path)

def parse_s3_path(s3_path: str):
    s3_path_no_scheme = s3_path.replace("s3://","")
    bucket, _, key=s3_path_no_scheme.partition('/') # ex: (temporal-dev)---> bucket / (folder1) ---> idont care about this /(file.pdf)--> key 

    return bucket, key