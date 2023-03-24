from azure.core.exceptions import ResourceNotFoundError
from blob_connect import upload_to_blob, download_from_blob, AzureBlob
import pandas as pd
import os
import argparse
from urllib.parse import urlparse
import json
import traceback
#from pathvalidate import sanitize_filepath
import zipfile
import shutil
from datetime import datetime


parser = argparse.ArgumentParser(description='Download file from azure blob storage.')
parser.add_argument('--save_path', type=str,   required = True,  help='')
parser.add_argument('--project_code',  type=str,   required = True,  help='')
parser.add_argument('--commit_id',  type=str,   required = True,  help='')
parser.add_argument('--excel_input', type=str,  required = True, help='input excel from download')
parser.add_argument('--start_date', type=str,  required = True, help='start date: in format YYYYMMDD-0000')
parser.add_argument('--stop_date', type=str,  required = True, help='stop date: in format YYYYMMDD-0000')
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
    #save_file = sanitize_filepath(save_file, platform='auto')
    with open(save_file, "wb") as file:
      file.write(file_content)
      return True, save_file
  except ResourceNotFoundError:
    print(traceback.format_exc())
    #raise ResourceNotFoundError(f"The specified {endpoint} does not exist.")
    return True, save_file

def load_input(filename, sheet_name, commit_id,
               start_time, stop_time):
  
  commit_id = commit_id.replace(" ","").split(",")
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
  
  check_col = ['created_time', 'excel', 'GIT_INFO']
  for c in check_col:
    assert c in pandas_data, f'{c} not found in columm.'

  for ind in pandas_data.index:
    excel_path = pandas_data['excel'][ind]
    git_info   = eval(pandas_data['GIT_INFO'][ind].replace("null","None"))
    created_time = datetime.strptime(pandas_data['created_time'][ind], '%Y-%m-%d %H:%M:%S')

      
    if (git_info['commit'] in commit_id) and (start_time<created_time) and (created_time < stop_date):
      print(f'{start_time} ---- {created_time} ---- {stop_date} ---- {git_info["commit"]}') 
      yield excel_path, git_info
    
def find_files(filename, search_path):
  result = []

  # Wlaking top-down from the root
  for root, dir, files in os.walk(search_path):
    if filename in files:
      result.append(os.path.join(root, filename))
  return result

  


if __name__ == '__main__':
  print(f'Inputs: ')
  print(f'project_code: {args.project_code}')
  print(f'commit_id: {args.commit_id}')
  print(f'save_path: {args.save_path}')
  print(f'excel_input: {args.excel_input}')
  print(f'start_date: {args.start_date}')
  print(f'stop_date: {args.stop_date}')
  start_date = datetime.strptime(args.start_date, '%Y%m%d-%H%M')
  stop_date  = datetime.strptime(args.stop_date, '%Y%m%d-%H%M')
  print(start_date)
  print('='*10)

  assert args.project_code in project_field_names, f'{args.project_code} is not define in mapping'
  # make download path:
  zip_path=os.path.join(args.save_path,'result_zip')
  zip_temp=os.path.join(args.save_path,'temp_zip')
  commit_id_folder = '-'.join([s[:5] for s in args.commit_id.replace(" ","").split(",")])
  save_path=os.path.join(args.save_path,'outputs',f'{args.project_code}',f'{commit_id_folder}')
  if os.path.exists(zip_temp):
    print(f'delete temp: {zip_temp}')
    shutil.rmtree(zip_temp, ignore_errors=True)
  os.makedirs(zip_temp, exist_ok=True)
  os.makedirs(save_path, exist_ok=True)


  # filter and download
  local_paths=[]
  for excel_path, _ in load_input(args.excel_input, 'summary', args.commit_id, start_date, stop_date):
    status, local_path = download_azblob(excel_path, zip_path)
    print(status, local_path )
    if status:
      local_paths.append(local_path)

  assert len(local_paths) !=0, f'There is not date in {args.commit_id} {start_date} {stop_date}'

  # extract all file
  for zip_file in local_paths:
    with zipfile.ZipFile(zip_file, 'r') as zip_obj:
      zip_obj.extractall(zip_temp)

  summary_data = {'field_name':[],
                  'eds':[],
                  'accuracy': [],
                  '#char': [],
                  '#correct_char': []
                }

  for fieldname in project_field_names[args.project_code]:
    search_filename=find_files(f'{fieldname}.csv',zip_temp)
    merge_field_csv=os.path.join(save_path,f'{fieldname}.csv')
    print(f'merging: {merge_field_csv}')
    df = pd.concat((pd.read_csv(f, index_col=[0]) for f in search_filename), ignore_index=True)
    df.to_csv(merge_field_csv)
    summary_data['field_name'].append(fieldname)
    summary_data['eds'].append(df['eds'].mean())
    summary_data['accuracy'].append(df['accuracy'].mean())
    summary_data['#char'].append(df['#char'].sum())
    summary_data['#correct_char'].append(df['#correct_char'].sum())

  print('Averaging')
  pd_summary = pd.DataFrame(summary_data)
  average_eds = pd_summary['eds'].mean()
  average_acc = pd_summary['accuracy'].mean()
  sum_char    = pd_summary['#char'].sum()
  sum_correct_char = pd_summary['#correct_char'].sum()
  char_acc    = sum_correct_char/sum_char


  summary_data['field_name'].append('Averaging')
  summary_data['eds'].append(average_eds)
  summary_data['accuracy'].append(average_acc)
  summary_data['#char'].append('')
  summary_data['#correct_char'].append(char_acc)
  
  summary_data = pd.DataFrame(summary_data)
  summary_filename_csv=os.path.join(save_path,f'summary_performance.csv')
  print(f'Saving: {summary_filename_csv}')

  summary_data.to_csv(summary_filename_csv)
    
