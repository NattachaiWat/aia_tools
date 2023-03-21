from azure.core.exceptions import ResourceNotFoundError
from blob_connect import upload_to_blob, download_from_blob, AzureBlob
import pandas as pd
import os
import argparse
from urllib.parse import urlparse
import json
import traceback
from pathvalidate import sanitize_filepath
import zipfile


parser = argparse.ArgumentParser(description='Download file from azure blob storage.')
parser.add_argument('--save_path', type=str,   required = True,  help='')
parser.add_argument('--project_code',  type=str,   required = True,  help='')
parser.add_argument('--commit_id',  type=str,   required = True,  help='')
parser.add_argument('--excel_input', type=str,  required = True, help='input excel from download')
args = parser.parse_args()


project_field_names = {
  "AIA008": ['BILLDATE', 
             'CLAIMANTNAME', 'CLAIMANTSURNAME',
             'HOSPITALNAME', 'HOSPITALNAME_AIA',
             'GRANDTOTAL', 'NETAMOUNT', 'GROSSAMOUNT','DISCOUNTAMOUNT',
             'BILLINGITEMS', 'ITEMSDETAIL' , 'ITEMSDETAIL_AIA']
}

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
    save_file = sanitize_filepath(save_file, platform='auto')
    with open(save_file, "wb") as file:
      file.write(file_content)
      return True, save_file
  except ResourceNotFoundError:
    print(traceback.format_exc())
    #raise ResourceNotFoundError(f"The specified {endpoint} does not exist.")
    return True, save_file

def load_input(filename, sheet_name, commit_id):
  assert os.path.exists(filename), f'{filename} is not exists'
  sheetnames = pd.read_excel(filename,  sheet_name = None,
                                engine = 'openpyxl',
                                na_filter=False,
                                dtype=str)
  assert sheet_name in sheetnames, f'{sheet_name} is not in sheetname: {filename}'
  pandas_data= pd.read_excel(filename,
                            sheet_name = sheet_name,
                            engine = 'openpyxl',
                            na_filter = False,
                            dtype=str)
  
  check_col = ['excel', 'GIT_INFO']
  for c in check_col:
    assert c in pandas_data, f'{c} not found in columm.'

  for ind in pandas_data.index:
    excel_path = pandas_data['excel'][ind]
    git_info   = eval(pandas_data['GIT_INFO'][ind].replace("null","None"))
    if commit_id == git_info['commit']:
      yield excel_path, git_info
    
def find_files(filename, search_path):
  result = []

  # Wlaking top-down from the root
  for root, dir, files in os.walk(search_path):
    if filename in files:
      result.append(os.path.join(root, filename))
  return result

  


if __name__ == '__main__':
  print(f'project_code: {args.project_code}')
  print(f'commit_id: {args.commit_id}')
  print(f'save_path: {args.save_path}')
  print(f'excel_input: {args.excel_input}')

  assert args.project_code in project_field_names, f'{args.project_code} is not define in mapping'
  # make download path:
  zip_path=os.path.join(args.save_path,'result_zip')
  zip_temp=os.path.join(args.save_path,'temp_zip')
  os.makedirs(zip_temp, exist_ok=True)


  local_paths=[]
  for excel_path, _ in load_input(args.excel_input, 'summary', args.commit_id):
    status, local_path = download_azblob(excel_path, zip_path)
    print(status, local_path )
    if status:
      local_paths.append(local_path)

  # extract all file
  for zip_file in local_paths:
    with zipfile.ZipFile(zip_file, 'r') as zip_obj:
      zip_obj.extractall(zip_temp)

  
  for fieldname in project_field_names[args.project_code]:
    search_filename=find_files(f'{fieldname}.csv',zip_temp)
    print(search_filename)



  # print("save_path:", args.save_path)
  # print("url:", args.url)

  # status, list_path = download_azblob(url = args.url,
  #                         local_path = args.save_path)
  
  # if status:
  #   print(f"saved: {list_path}")

  # with open(sanitize_filepath(list_path, platform='auto'),'r') as fp:
  #   for path in fp.readlines():
  #     path = path.replace('\n','')
  #     status, local_path = download_azblob(url = path,
  #                           local_path = args.save_path)
  #     if status:
  #       print(f"saved: {local_path}")
  #     else:
  #       print(f'Cannot download: {path}')
