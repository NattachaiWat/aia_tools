import os
from typing import List, Tuple
import pandas as pd 
import json
import argparse
import shutil
from tqdm import tqdm
from pathvalidate import sanitize_filepath



def read_excel(filename):
  excel_data = pd.read_excel(filename, sheet_name = None)
  return excel_data
def save_excel(filename:str, excel_datas: List):
  df_single = pd.concat([single for single, _ in excel_datas], ignore_index= False)
  df_single.image_id = range(df_single.shape[0])

  df_billitems = pd.concat([multiple for _, multiple in excel_datas], ignore_index= False)
  df_billitems.image_id = df_billitems["filename"].apply(lambda x: df_single[df_single["filename"] == x]["image_id"].index[0])
  
  with pd.ExcelWriter(filename) as writer:
    df_single.to_excel(writer, 
                      sheet_name='single', engine='openpyxl', index=False)
    df_billitems.to_excel(writer, 
                      sheet_name='BILLINGITEMS', engine='openpyxl', index=False)

def list_all_images(excel_data, image_path):
  all_images = list()
  for sheetname, tables  in excel_data.items():
    for filename  in tables['filename'].values:
      if filename not in all_images:
        if os.path.exists(os.path.join(image_path, filename)):
          all_images.append(filename)
  return all_images

def duplicate_images(images_filename: List, image_path: str, copy_str: str):
  for filename in images_filename:
    base, ext = os.path.splitext(filename)
    source_file = os.path.join(image_path,filename)
    target_file = sanitize_filepath(os.path.join(image_path,f'{base}_{copy_str}{ext}'), platform='auto')
    if not os.path.exists(target_file):
      shutil.copyfile(source_file, target_file)

def new_filename(filename,copy_str):
  base, ext = os.path.splitext(filename)
  return f'{base}_{copy_str}{ext}'

def rename_filename(excel_data, copy_str):
  df_single = excel_data['single'].copy()
  df_multiple = excel_data['BILLINGITEMS'].copy()
  df_single.filename = df_single["filename"].apply(lambda x: new_filename(filename=x, copy_str=copy_str))
  df_multiple.filename = df_multiple["filename"].apply(lambda x: new_filename(filename=x, copy_str=copy_str))

  return df_single, df_multiple
  

    
    


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Upload image to azure blob storage.')

  parser.add_argument('--project_code', type=str)
  parser.add_argument('--input_excel', type=str,  required=True)
  parser.add_argument('--image_path', type=str,  required=True)
  parser.add_argument('--output_excel', type=str,  required=True)
  parser.add_argument('--duplicate', type=int, required=True)

  args = parser.parse_args()
  assert os.path.exists(args.input_excel), f'{args.input_excel} is not found!'
  assert os.path.exists(args.image_path), f'{args.image_path} is not found!'


  excel_data = read_excel(args.input_excel)
  images_filename = list_all_images(excel_data = excel_data,
                                image_path = args.image_path)
  excel_datas = list()
  for i in tqdm(range(0,args.duplicate)):
    duplicate_images(images_filename, args.image_path, f'{i:02d}')
    excel_datas.append(rename_filename(excel_data,f'{i:02d}'))                             
  
  # excel_datas = [(excel_data['single'].copy(),excel_data['BILLINGITEMS'].copy()),
  #                 (excel_data['single'].copy(),excel_data['BILLINGITEMS'].copy())]  
  save_excel(filename=args.output_excel,
             excel_datas=excel_datas)    
