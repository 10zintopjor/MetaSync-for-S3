from github import Github
import os
from openpecha.utils import load_yaml
from pathlib import Path
import boto3
import hashlib
import json
import yaml
import datetime
import csv


os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "~/.aws/credentials"
OCR_OUTPUT_BUCKET = "ocr.bdrc.io"
S3 = boto3.resource("s3")
S3_client = boto3.client("s3")
ocr_output_bucket = S3.Bucket(OCR_OUTPUT_BUCKET)

def get_s3_folder_prefix(wlname):
    md5 = hashlib.md5(str.encode(wlname))
    two = md5.hexdigest()[:2]
    return 'Works/{two}/{RID}/google_books/batch_2022'.format(two=two, RID=wlname)

def downloadDirectoryFroms3(work_id):
    print(f"DOWNLOADING {work_id}")
    gb_path = get_s3_folder_prefix(work_id)
    info_path = f"{gb_path}/info.json"
    if not os.path.exists(os.path.dirname(info_path)):
        os.makedirs(os.path.dirname(info_path))
    S3_client.download_file(OCR_OUTPUT_BUCKET,info_path,info_path)
    return info_path


def read_json_file(file_path):
    with open(file_path, 'r') as file:
        json_content = file.read()
        data = json.loads(json_content)
        return data
    
def get_new_content(info_path,meta):
    ocr_import_info = read_json_file(info_path)
    meta["ocr_import_info"] = ocr_import_info
    meta_yml = yaml.safe_dump(meta, default_flow_style=False, sort_keys=False,  allow_unicode=True)
    return meta_yml
    

def update_file(repo_name,info_path):
    branch="master"
    file_path= f"{repo_name}.opf/meta.yml"
    token = os.getenv("GITHUB_TOKEN")
    g = Github(token)
    repo = g.get_repo(f"OpenPecha-Data/{repo_name}")
    file = repo.get_contents(file_path, ref=branch)
    file_content = file.decoded_content.decode('utf-8')
    file_dict = yaml.safe_load(file_content)
    new_content = get_new_content(info_path,file_dict)
    repo.update_file(
        file_path,
        f"Updating file: {file_path}",
        new_content,
        file.sha,
        branch=branch
    )



if __name__ == "__main__":
    file_path="repos.csv"
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            repo_name = row[0]
            work_id = row[1]
            print(work_id)
            info_path = downloadDirectoryFroms3(work_id)
            update_file(repo_name,info_path)