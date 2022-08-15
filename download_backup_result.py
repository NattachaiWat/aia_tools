from azure.core.exceptions import ResourceNotFoundError
from blob_connect import upload_to_blob, download_from_blob, AzureBlob
import pandas as pd
import os
import argparse
from urllib.parse import urlparse
from persistance import cache_list_manager
import json
import traceback
import numpy as np
from pathvalidate import sanitize_filepath
import re

parser = argparse.ArgumentParser(description='Download file from azure blob storage.')
parser.add_argument('--save_path', type=str, default='.', help='')
parser.add_argument('--url', type=str,  required = True, help='')
parser.add_argument('--project_code',  type=str,  required = True, help='')
args = parser.parse_args()

def remove_special_char(string):
  new_string = re.sub(r"[\*\[\]]","",string)
  return new_string

def load_json(filename):
  with open(filename, 'r', encoding='utf8') as json_file:
    data = json.load(json_file)
    return data

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


def save_excel(raw_result, 
                evaluate_result,
                mapping_fields,
                pCache,
                filename):
  
  group_billitems = [ "code", 
                      "itemsDetail_aia", 
                      "netAmount",
                      "discountAmount",
                      "grossAmount",
                      "notCovered",
                      "covered",
                      "itemsDetail",
                      "billingItems"]

  header_in_raws = ['filename',
                    'gt',
                    'ztrus_ocr', 
                    'eds', 
                    'accuracy',
                    '#char',
                    '#correct_char',
                    'confident',
                    'text_type',
                    'iqa',
                    'block_height',
                    'number_block',
                    'width',
                    'height',
                    'x_dpi',
                    'y_dpi',
                    'has_answer']

  header_in_raws_billitems = ['filename',
                    'item_id', 
                    'gt',
                    'matching_item_id', 
                    'ztrus_ocr', 
                    'eds', 
                    'accuracy',
                    '#char',
                    '#correct_char',
                    'confident',
                    'text_type',
                    'iqa',
                    'block_height',
                    'number_block',
                    'width',
                    'height',
                    'x_dpi',
                    'y_dpi',
                    'has_answer']

  ON_EXCEL_ZTRUS_INFO = os.getenv('ON_EXCEL_ZTRUS_INFO', None)

  # enable ztrus info in column
  if ON_EXCEL_ZTRUS_INFO is None:
    # disable ztrus_info in list
    if os.getenv("TRAINNER_PROJECT_CODE") in ['AIA008']:
      on_ztrus_info = False
    else:
      on_ztrus_info = True
  elif isinstance(ON_EXCEL_ZTRUS_INFO, str):
    # control on and off ztrus info by ON_EXCEL_ZTRUS_INFO
    on_ztrus_info = ON_EXCEL_ZTRUS_INFO.lower() == 'on'

  if on_ztrus_info:
    header_in_raws.append('ztrus_info')
    header_in_raws_billitems.append('ztrus_info')

  with pd.ExcelWriter(filename) as writer:
    try:
      pandas_output = {'summary_performance': { 'field_name': list(),
                                                'eds': list(),
                                                'accuracy': list(),
                                                '#char': list(),
                                                '#correct_char': list()}}

                                    

      for field_name, performace in evaluate_result.items():
        
        pandas_output['summary_performance']['field_name'].append(mapping_fields[field_name])
        pandas_output['summary_performance']['eds'].append(performace['eds']) 
        pandas_output['summary_performance']['accuracy'].append(performace['accuracy'])
        pandas_output['summary_performance']['#char'].append(performace['#char'])
        pandas_output['summary_performance']['#correct_char'].append(performace['#correct_char'])


        # raw data in each field
        if field_name not in group_billitems:
          pandas_output[mapping_fields[field_name]] = {header: list() 
                                                      for header in header_in_raws }
        else:
          pandas_output[mapping_fields[field_name]] = {header: list() 
                                                      for header in header_in_raws_billitems }
      # find average 
      
      # calculate
      accum_correct_char = np.sum(pandas_output['summary_performance']['#correct_char'])
      total_char = np.sum(pandas_output['summary_performance']['#char'])
      average_acc = np.average(pandas_output['summary_performance']['accuracy'])
      average_eds = np.average(pandas_output['summary_performance']['eds'])

      # printout
      pandas_output['summary_performance']['field_name'].append('Averaging')
      pandas_output['summary_performance']['eds'].append(float(average_eds)) 
      pandas_output['summary_performance']['accuracy'].append(float(average_acc))
      pandas_output['summary_performance']['#char'].append('')
      pandas_output['summary_performance']['#correct_char'].append(float(accum_correct_char/total_char) if total_char != 0 else 'N/A')

      # write to excel
      pd_output = pd.DataFrame(pandas_output['summary_performance'])
      pd_output.to_excel(writer, 
                          engine='openpyxl',
                          index=False, 
                          sheet_name = remove_special_char('summary_performance'))
      del pandas_output['summary_performance']


      print('====> save excel')
      for field_name, raw_data in raw_result.items():
        for d in raw_data:
          data = pCache[d]
          #print(d, field_name, list(data.keys()))
          if field_name not in group_billitems:
            for header in header_in_raws:
              if header in data:
                pandas_output[mapping_fields[field_name]][header].append(data[header])
              else:
                pandas_output[mapping_fields[field_name]][header].append('')

          else:
            for header in header_in_raws_billitems:
              if header in data:
                pandas_output[mapping_fields[field_name]][header].append(data[header])
              else:
                pandas_output[mapping_fields[field_name]][header].append('')
        
        pd_output = pd.DataFrame(pandas_output[mapping_fields[field_name]])
        pd_output.to_excel(writer, 
                          engine='openpyxl',
                          index=False, 
                          sheet_name = remove_special_char(mapping_fields[field_name]))
                          
        del pandas_output[mapping_fields[field_name]]
      writer.save()
    except Exception as err:
      print(traceback.format_exc())


if __name__ == '__main__':
  print("save_path:", args.save_path)
  print("url:", args.url)
  print("Project Code:", args.project_code)

  status, list_path = download_azblob(url = args.url,
                          local_path = args.save_path)
  
  if status:
    print(f"saved: {list_path}")

    with open(sanitize_filepath(list_path, platform='auto'),'r') as fp:
      for path in fp.readlines():
        path = path.replace('\n','')
        status, local_path = download_azblob(url = path,
                              local_path = args.save_path)
        if status:
          print(f"saved: {local_path}")
        else:
          print(f'Cannot download: {path}')
    
    raw_result = os.path.join(args.save_path, 'research', args.project_code, 'lastest_performance', 'raw_result.json')
    mapping_fields = os.path.join(args.save_path, 'research', args.project_code, 'lastest_performance', 'mapping_fields.json')
    databasedb_path = os.path.join(args.save_path, 'research', args.project_code, 'lastest_performance')
    evaluate_result = os.path.join(args.save_path, 'research', args.project_code, 'lastest_performance', 'evaluate_result.json')
    excel_result = os.path.join(args.save_path, 'research', args.project_code, 'lastest_performance', f'{args.project_code}_performance.xlsx')


    assert os.path.exists(raw_result), f'raw_result is not found.'
    assert os.path.exists(mapping_fields), f'mapping_fields is not found.'
    assert os.path.exists(databasedb_path), f'databasedb_file is not found.'
    assert os.path.exists(evaluate_result), f'evaluate_result is not found.'

    raw_result = load_json(raw_result)
    mapping_fields = load_json(mapping_fields)
    evaluate_result = load_json(evaluate_result)
    raw_result_dbname = cache_list_manager(cache_name = 'databasedb', cache_path = databasedb_path)

    save_excel(raw_result = raw_result,
                evaluate_result = evaluate_result,
                mapping_fields = mapping_fields,
                pCache = raw_result_dbname,
                filename = excel_result)
