from genericpath import exists
from azure.core.exceptions import ResourceNotFoundError
from blob_connect import upload_to_blob, download_from_blob, AzureBlob
import pandas as pd
import os
import argparse
from urllib.parse import urlparse
import json
import traceback

parser = argparse.ArgumentParser(description='Download file from azure blob storage.')
parser.add_argument('--save_path', type=str, default='.', help='')
parser.add_argument('--url', type=str,  required = True, help='')
args = parser.parse_args()


def download_azblob(url, local_path):
  o = urlparse(url, allow_fragments=False)
  
  container_name = o.path.lstrip('/').split('/')[0]
  subfolder = o.path.lstrip('/').split('/')[0]
  endpoint = '/'.join(o.path.lstrip('/').split('/')[1:])

  save_file = os.path.join(local_path, endpoint)
  save_path = os.path.dirname(save_file)
  os.makedirs(save_path, exist_ok = True)
  
  conn = AzureBlob()

  blob_container = conn._BLOB_CLIENT.get_container_client(container_name)
  try:
    file_content = blob_container.get_blob_client(endpoint).download_blob().readall()
    with open(save_file, "wb") as file:
      file.write(file_content)
      return True, save_file
  except ResourceNotFoundError:
    print(traceback.format_exc())
    #raise ResourceNotFoundError(f"The specified {endpoint} does not exist.")
    return True, save_file



if __name__ == '__main__':
  print("save_path:", args.save_path)
  print("url:", args.url)

  status, list_path = download_azblob(url = args.url,
                          local_path = args.save_path)
  if status:
    print(f"saved: {list_path}")

  with open(list_path,'r') as fp:
    for path in fp.readlines():
      path = path.replace('\n','')
      status, local_path = download_azblob(url = path,
                            local_path = args.save_path)
      if status:
        print(f"saved: {local_path}")
      else:
        print(f'Cannot download: {path}')
