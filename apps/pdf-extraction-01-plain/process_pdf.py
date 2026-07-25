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

# this func to Download a pdf from s3 . Returns local file path. 

def download_s3_file(s3_path: str) -> str:
    
    bucket,key = parse_s3_path(s3_path)

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

    return local_path



# okay we downloeded file , the next is step in extracted content from file 

def extract_to_markdown (local_pdf_path:str) -> str:
    logger.info(f"Extaction complete {local_pdf_path}")

    try:
        markdown_text = pymupdf4llm.to_markdown(local_pdf_path)
        logger.info(f"Extraction complet {len(markdown_text)} characters")
        return markdown_text
    
    except Exception as e: 
        logger.error(f"check the locla_pdf_path {local_pdf_path} : {e}")

        raise

# upload the markdown file to S3

def upload_markdown(markdown_text: str, original_s3_path: str) -> str:
    bucket,key = parse_s3_path(original_s3_path)
    md_key = key.replace(".pdf",".md")
    
    logger.info(f"uploading markdown -> s3://{bucket}/{md_key}")
    s3_client = get_S3_client()
   
    s3_client.put_object(
        Bucket = bucket,
        Key = md_key,  

        Body = markdown_text.encode("utf-8"),
        ContentType = "text/markdown"   
    )
    output_path = f"s3://{bucket}/{md_key}"
    logger.info(f'Upload Complete: {output_path}')
    return output_path

# run the pipeline 

def process_pdf(s3_input_path:str)->str:
    logger.info(f"Starting pipeline for: {s3_input_path}")
    
    local_pdf = download_s3_file(s3_input_path)    
    markdown = extract_to_markdown(local_pdf)
    output_s3 = upload_markdown(markdown,s3_input_path)

    os.remove(local_pdf)

    logger.info(f"pipeline complete. output: {output_s3}")
    return output_s3




if __name__ == "__main__":

    output_s3 = process_pdf(
        sys.argv[1]
    )

    print(f'\nDone! markdown saved to : {output_s3}')
# ``` bash
# $ python process_pdf.py s3://temporal-101/files/test-file.pdf
# ```
