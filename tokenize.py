from transformers import AutoTokenizer
from datasets import load_dataset
import os
from datasets import disable_caching
import shutil
import subprocess
from dotenv import load_dotenv
load_dotenv()
import sys
ds = sys.argv[1:]
disable_caching()

tokenizer = AutoTokenizer.from_pretrained("johngiorgi/declutr-base")

BATCH_SIZE = 32
BATCH_SIZE_SAVING = 1000
MAX_LENGTH = 1024
NUM_PROC = 50


def tokenize(text_list:list):
    encoded_input = tokenizer(      
        text_list, max_length=MAX_LENGTH, truncation=True
    )
    return encoded_input


def process_file(d:str):
    if not os.path.exists(d):
        os.makedirs(d)


    URL = f'https://the-eye.eu/public/AI/pile/train/{d}.jsonl.zst'

    dataset = load_dataset("json", data_files=URL, split="train")
    dataset.cleanup_cache_files()

    # delete cache for memory saving
    try:
        shutil.rmtree('/home/ubuntu/.cache/huggingface/datasets/downloads')
    except FileNotFoundError:
        print('FileNotFound')

    dataset = dataset.map(
        lambda x: tokenize(x['text']),
        batched=True,
        num_proc=NUM_PROC,
        batch_size=BATCH_SIZE
    )

    dataset.to_parquet(f'{d}/{d}.parquet', batch_size=BATCH_SIZE_SAVING)

    # delete cache for memory saving
    try:
        shutil.rmtree('/home/ubuntu/.cache/huggingface')
    except FileNotFoundError:
        print('FileNotFound')
    
    # copy to S3 instance
    subprocess.run(['aws', 's3', 'cp', d, os.environ['s3_dir'], '--recursive'])

    # delete cache for memory saving
    try:
        shutil.rmtree(d)
    except FileNotFoundError:
        print('FileNotFound')


if __name__=='main':
    for d in ds:
        print(f'SAVING TO {d}')
        process_file(d)
