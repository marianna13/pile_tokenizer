import os
import requests
from tqdm import tqdm
import sys


print("User Current Version:-", sys.version)

def download(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = url.split('/')[-1].replace(" ", "_")
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    total_size_in_bytes = int(r.headers.get('content-length', 0))
    block_size = 1024*1024*8  # 8 Gibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=block_size):
                if chunk:
                    progress_bar.update(len(chunk))
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
        progress_bar.close()
    else:  # HTTP status code 4XX/5XX
        print(f"Download failed: status code {r.status_code}\n{r.text}")

if __name__=='main':
    URL = 'https://the-eye.eu/public/AI/pile/train/03.jsonl.zst'
    download(URL, dest_folder="train")
