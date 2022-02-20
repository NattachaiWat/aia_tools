import os
import sys
import os.path as osp
import traceback
from typing import Dict, List, Tuple
import json
import numpy as np
import pandas as pd
from multiprocessing.pool import ThreadPool

from blob_connect import upload_to_blob
from urllib.parse import urlparse

NUM_PROCESSORS = 4

single_billitems_columns = ['image_id', 'filename', 'CLAIM_NUMBER', 
                                'OCCURRENCE', 'HOSPITALNAME', 'HOSPITALNAME_AIA', 
                                'BILLDATE', 'CLAIMANTNAME', 'CLAIMANTSURNAME', 'GRANDTOTAL']
multiple_billing_columns = ['image_id', 
                                'filename', #'items_id',
                                'CLAIM_NUMBER', 'OCCURRENCE',
                                'ITEMSDETAIL', 'ITEMSDETAIL_AIA', 'GROSSAMOUNT',
                                'DISCOUNTAMOUNT',	'NETAMOUNT']

def save_json(json_data, path_output_json):
    with open(path_output_json, 'w', encoding='utf8') as json_file:
        json.dump(json_data.copy(), json_file, ensure_ascii=False, indent=4)

def check_file_image(main_folder_az, folder=None,header_name="filename"):
    list_path_file_image,list_file_name_image,list_path_blob_image = find_file(main_folder_az, folder=folder,type_file=['tiff','tif', 'jpg', 'jpeg', 'bmp', 'png', 'pdf'],remove_main_path=True)
    list_path_excel,_,list_path_blob_excel = find_file(main_folder_az, folder=folder,type_file=["xlsx"],remove_main_path=True)
    print('-'*10,'Excel files found','-'*10)
    print(str(list_path_blob_excel[:3])[:-1]+'...','len:',len(list_path_blob_excel))
    print('-'*20)
    print('-'*10,'Image files found','-'*10)
    print(str(list_path_blob_image[:3])[:-1]+'...','len:',len(list_path_blob_image))
    print('-'*20)

    error_check = False
    num_filename_excel = 0
    list_file_name_image_check_local = []
    list_file_name_image_check_blob = []

    images_no_found = list()
    checking_string = list()
    
    excel_ok = list()
    format_errors = list()
    for path_file_read in list_path_excel:
        excel_data = pd.read_excel(path_file_read, sheet_name = None)
        ignore_file = False
        images_in_excel = dict()
        for sheetname, tables  in excel_data.items():
            if sheetname.lower() not in ['single', 'billingitems']:
                ignore_file = True
                checking_string.append(f'Warning:Wrong format: {sheetname} not in ["SINGLE", "{"billingitems".upper()}"] in {path_file_read}')
                format_errors.append({'error_type': 'sheetname is wrong',
                                        'messages': f'{path_file_read}:{sheetname}'})
                break

            if header_name not in tables.columns:
                ignore_file = True
                checking_string.append(f'Warning:Wrong format: the filename column is not in {path_file_read} in [{sheetname}]')
                format_errors.append({'error_type': 'filename is not found',
                                        'messages': f'{path_file_read}:{sheetname}'})
                break
              
            # check wrong fields
            if sheetname.lower() == 'single':
                possible_column_set = set(single_billitems_columns)
            elif sheetname.lower() == 'billingitems':
                possible_column_set = set(multiple_billing_columns)
            

            # check not match
            missing_fieldnames = possible_column_set - set(tables.columns.values)
            if len(missing_fieldnames) > 0:
                ignore_file = True
                for col_name in missing_fieldnames:
                    checking_string.append(f'Warning: column name is missing in {path_file_read} in [{sheetname}]')
                    format_errors.append({'error_type': 'column name is missing',
                                        'messages': f'{path_file_read}:{sheetname}:{col_name}'})
                break
                
            noise_fieldnames = set(tables.columns.values)-possible_column_set
            for col_name in noise_fieldnames:
                checking_string.append(f'Warning: column name is wrong in {path_file_read} in [{sheetname}]')
                format_errors.append({'error_type': 'column name is wrong',
                                        'messages': f'{path_file_read}:{sheetname}:{col_name}'})
            
            if sheetname.lower() not in images_in_excel:
                images_in_excel[sheetname.lower()] = list()
                images_in_excel[sheetname.lower()] += list(tables.get(header_name).values)
            

        if ignore_file:
            continue
        excel_ok.append(path_file_read)

        


        for sheetname, d in images_in_excel.items():
            images_in_excel[sheetname] = list(set(d))
            num_filename_excel += len(images_in_excel[sheetname])
        
        for sheetname, data in images_in_excel.items():
            for file_name_image in data:
   
                if file_name_image in list_file_name_image:
                    if str(file_name_image) == 'nan':
                        print(file_name_image)
                    value_path_image_local = list_path_file_image[list_file_name_image.index(file_name_image)]
                    value_path_image_blob = list_path_blob_image[list_file_name_image.index(file_name_image)]
                    list_file_name_image_check_local.append(value_path_image_local)
                    list_file_name_image_check_blob.append(value_path_image_blob)
                else:
                    if str(file_name_image) == 'nan' or file_name_image is None:
                        _file_name_image = 'null'
                    else:
                        _file_name_image = file_name_image
                    printout = f'WARNING: image is not found in"{path_file_read}", "{_file_name_image}" is not found from "{folder}"'
                    format_errors.append({'error_type': 'images are not found',
                                            'messages': f'{path_file_read}:{sheetname}:{_file_name_image}'})
                    checking_string.append(printout)
                    images_no_found.append(file_name_image)
                    error_check = True
        
    return list_file_name_image_check_local, \
            excel_ok, \
            list_file_name_image_check_blob, \
            list_path_blob_excel, \
            error_check, \
            list(set(images_no_found)), \
            checking_string, \
            format_errors  

def find_file(main_folder_az, folder=None, type_file=["xlsx"],remove_main_path=False):
    list_path_blob_file = []
    list_path_file = []
    list_file_name = []
    if folder is None:
        return list_path_file
    
    folder_find_images = '/'.join([folder,'images'])
    folder_find_excel = '/'.join([folder,'excel'])

    for main_folder,sub_folder,list_file in os.walk(folder):
        main_folder = str(main_folder).replace('\\','/') 
        if str(main_folder).find(folder_find_images) == 0 or (str(main_folder).find(folder_find_excel) == 0 and "xlsx" in type_file):
            for file in list_file:
                type_file_split = str(os.path.basename(file)).split('.')[-1]
                if str(type_file_split).lower() in str(type_file).lower():
                    path_blob = main_folder
                    if remove_main_path == True:
                        path_blob = str(main_folder)[len(folder)+1:]
                    list_path_blob_file.append('/'.join([main_folder_az,path_blob,file]))
                    list_path_file.append('/'.join([main_folder,file]))
                    list_file_name.append(os.path.basename(file))

    return list_path_file,list_file_name,list_path_blob_file

def thread_upload(container_string:str, 
                  folder_blob_list:List[str], 
                  local_file_path_list:List[str], 
                  num_process:int=NUM_PROCESSORS) -> List[Tuple[bool, str]]:
    """ 
    use ThreadPool to upload multiple file to Azure Blob with upload_to_blob

    Args:
        container_string (str): container string
        folder_blob_list (List[str]): list of blob folder
        local_file_path_list (List[str]): list of local fille path
        num_process (int, optional): number of processor to be used. Defaults to NUM_PROCESSORS.

    Returns:
        List[Tuple[bool, str]]: list of tuple of (status, blob_url)
    """
    
    container_string_list = [container_string for _ in folder_blob_list]
    list_work = zip(container_string_list, folder_blob_list, local_file_path_list)
    print(f'Uploading: #{len(local_file_path_list)}')
    with ThreadPool(processes=num_process) as pool:
        result = pool.starmap(upload_to_blob, list_work)

    return result

def get_split_list(lst, size):
    return [list(i) for i in np.array_split(lst, size)]

def partition_billitem(list_path_excel:List[str], 
                        num_partition:int, 
                        images_no_found: List[str]) -> Tuple[List[Tuple[pd.DataFrame, pd.DataFrame]], 
                                                             List[str], List[dict]]:
    single_df_list = [[] for _ in range(num_partition)]
    billitem_df_list = [[] for _ in range(num_partition)]
    error_str = list()
    format_errors = list()
    for i, path in enumerate(list_path_excel):
        df_dict = pd.read_excel(path, sheet_name=None)
        sheetnames = df_dict.keys()
        print(sheetnames)
        for sheetname in sheetnames:
            if sheetname.lower() == 'single':
                df_single = df_dict.get(sheetname)
                #remove filename is not found
                df_single = df_single[~df_single['filename'].isin(images_no_found)]
            elif sheetname.lower() == 'billingitems':
                df_billitem = df_dict.get(sheetname)
                # remove filename is not found!
                df_billitem = df_billitem[~df_billitem['filename'].isin(images_no_found)]
            else:
                raise Exception(f'sheetname is not in [single, ({"billingitems".upper()})]')

        # looking for filename in billingitems sheet is not in filename in single
        filenames_in_single = list(set(df_single.filename.values))
        #filenames_in_single = [i.lower() for i in filenames_in_single]
        filenames_in_billitem = list(set(df_billitem.filename.values))
        #filenames_in_billitem = [i.lower() for i in filenames_in_billitem]
        image_files_missing = list()
        for filename in filenames_in_billitem:
            if filename not in filenames_in_single:
                image_files_missing.append(filename) 

        # remove file
        if len(image_files_missing) > 0:
            # clean missing filename
            df_billitem = df_billitem[~df_billitem['filename'].isin(image_files_missing)]
            # printout
            for image_missing in  image_files_missing:
                error_str.append(f'WARNGING: image file missing in {path}:  {image_missing} in billingitems sheet is not found in single sheet')
                format_errors.append({'error_type': 'image is not match between 2 sheets',
                                        'messages': f'{path}:{image_missing}'})
      
        
        idx_list = get_split_list(range(len(list(df_single.image_id.values))), num_partition)
        for j, idx in enumerate(idx_list):
            temp_df_single = df_single.iloc[idx]

            image_idx = temp_df_single.image_id.values
            temp_df_billingitem = df_billitem[df_billitem['image_id'].isin(image_idx)]

            single_df_list[j].append(temp_df_single)
            billitem_df_list[j].append(temp_df_billingitem)

    df_partition = []
    for i, (singel_part, billitem_part) in enumerate(zip(single_df_list, billitem_df_list)):
        if len(singel_part) > 0 and len(billitem_part) > 0:
            df_single = pd.concat(singel_part, ignore_index=True)
            df_billitem = pd.concat(billitem_part, ignore_index=True)
            try:
                df_single.image_id = range(df_single.shape[0])
                
                func_test = lambda x: df_single[df_single["filename"] == x]["image_id"].index

                # file not match
                missing_filename = list()
                for filename in df_billitem["filename"].values:
                    c = func_test(filename)
                    if len(c) == 0:
                        #print(f'checking--> {filename}, {c}: {len(c)}')
                        #error_code = f'WARNING: filename is not matched: {filename}: partion  {i}: len: {len(c)}'
                        #error_str.append(error_code)
                        missing_filename.append(filename)
                        # format_errors.append({'error_type': 'image is not match between billingitems and single in partition',
                        #                 'messages': f'{filename}:{image_missing}'})
                #remove filename
                df_billitem = df_billitem[~df_billitem['filename'].isin(missing_filename)]

                df_billitem.image_id = df_billitem["filename"].apply(lambda x: df_single[df_single["filename"] == x]["image_id"].index[0])
                df_partition.append((df_single, df_billitem))
            except Exception as err:
                print(f'Warning: partition in {i}:  {traceback.format_exc()}')
        
    return df_partition, error_str, format_errors

def partition_single(list_path_excel:List[str], num_partition:int,
                     images_no_found: List[str]) -> List[pd.DataFrame]:
    single_df_list = [[] for _ in range(num_partition)]
    for i, path in enumerate(list_path_excel):
        df_dict = pd.read_excel(path, sheet_name=None)
        if 'single' in df_dict:
            df_single = df_dict.get('single')
        elif 'SINGLE' in df_dict:
            df_single = df_dict.get('SINGLE')
        else:
            sheetnames = df_dict.keys()
            df_single = None
            for sheetname in sheetnames:
                if sheetname.lower() == 'single':
                    df_single = df_dict.get(sheetname)
                    break
            if df_single is None:
                continue
             
        df_single = df_single[~df_single['filename'].isin(images_no_found)]
        
        idx_list = get_split_list(range(len(list(df_single.image_id.values))), num_partition)
        for j, idx in enumerate(idx_list):
            temp_df_single = df_single.iloc[idx]
            single_df_list[j].append(temp_df_single)
    
    df_partition = []
    for i, singel_part in enumerate(single_df_list):
        if len(singel_part) > 0:
            df_single = pd.concat(singel_part, ignore_index=True)
            df_single.image_id = range(df_single.shape[0])
            
            df_partition.append(df_single)

    return df_partition, list(), list()


                                
def partition_excel(list_path_excel:List[str], 
                        num_partition:int, 
                        project_code:str, 
                        input_folder:str,
                        images_no_found: List[str]) -> Tuple[List[str], List[str], List[Dict]]:
    """
    แบ่งแต่ละ excel ไฟล์ที่อยู่ใน list_path_excel เป็นส่วนๆเป็นจำนวน num_partition ส่วน
    แล้วเอาส่วนที่ i ของแต่ละไฟล์มา merge รวมกันแล้ว save excel แต่ละส่วน
    Args:
        list_path_excel (List[str]): [description]
        num_partition (int): [description]
    """
    
    
    save_folder = osp.join(input_folder, "partition")
    if not osp.exists(save_folder):
        os.makedirs(save_folder)
    #else: # partition file exist
    #    excel_path_list = sorted([osp.join(save_folder,f) for f in os.listdir(save_folder) if f.endswith('.xlsx')])
    #    if excel_path_list:
    #        print('Partitioned Excel file already exist.')
    #        return excel_path_list
    
    temp_dict = list()
    for excel_file in list_path_excel:
        temp_dict += list(pd.read_excel(excel_file, sheet_name=None).keys())
        temp_dict = list(set(temp_dict))
    temp_dict = set([s.lower() for s in temp_dict])
    excel_path_list = []
    print(temp_dict)
    if temp_dict == {'single'}:
        # for single sheet
        #print('check: single')
        df_partition, error_str, format_errors = partition_single(list_path_excel, num_partition, images_no_found)
        for i, df_part in enumerate(df_partition):
            df_part = df_part.loc[:, ~df_part.columns.str.contains('^Unnamed')]
            save_path = f"{project_code}_{i}of{num_partition}.xlsx"
            excel_path_list.append(save_path)
            with pd.ExcelWriter(osp.join(save_folder, save_path)) as writer:
                df_part.to_excel(writer, sheet_name='single', engine='openpyxl', index=False)
    else: 
        # for billing items
        #print('check: billing items')
        df_partition, error_str,format_errors = partition_billitem(list_path_excel, num_partition, images_no_found)
        for i, (df_single, df_billitem) in enumerate(df_partition):
            # remove noisy column
            df_single = df_single[single_billitems_columns]
            df_billitem = df_billitem[multiple_billing_columns]
            #if 'items_id' not in df_billitem:
            #    df_billitem['items_id'] = range(len(df_billitem['filename'].index))
            filename_keys = set(df_billitem['filename'])
    
            for filename in filename_keys:
                nItems = len(df_billitem.loc[df_billitem['filename'] == filename].index)
                
                df_billitem.loc[df_billitem.filename == filename, ['items_id']] = range(nItems)
                #print(df_billitem.loc[df_billitem.filename == filename])

            save_path = f"{project_code}_{i}of{num_partition}.xlsx"
            excel_path_list.append(save_path)
            with pd.ExcelWriter(osp.join(save_folder, save_path)) as writer:
                df_single.to_excel(writer, sheet_name='single', engine='openpyxl', index=False)
                df_billitem.to_excel(writer, sheet_name='BILLINGITEMS', engine='openpyxl', index=False)
    
    return excel_path_list, error_str, format_errors

def get_az_endpoint(az_path: List):
    endpoint_list = list()
    for status, url in az_path:
        if status:
            o = urlparse(url, allow_fragments=False)
            subfolder = o.path.lstrip('/').split('/')[0]
            endpoint = '/'.join(o.path.lstrip('/').split('/')[1:])
            endpoint_list.append(endpoint)

    return endpoint_list

def get_imagefiles_from_excel(excel_file, local_image_path):
    sheetnames = pd.read_excel(excel_file,  sheet_name = None,
                                engine = 'openpyxl',
                                na_filter=False)
    image_list = list()
    for sheetname in sheetnames:
        pandas_data= pd.read_excel(excel_file,
                                              sheet_name = sheetname,
                                              engine = 'openpyxl',
                                              na_filter = False,
                                              dtype=str)
        if 'filename' in  pandas_data:
            if len(pandas_data.get('filename')) > 0:                             
                image_list += list(pandas_data.get('filename'))
        elif 'filename'.upper() in pandas_data:
            if len(pandas_data.get('filename'.upper())) > 0:   
                image_list += list(pandas_data.get('filename'.upper()))
    fullpath = []
    for image_file in list(set(image_list)):
        fullpath.append(os.path.join(local_image_path,image_file))
    return fullpath

def edit_image_id(excel_file: str, partitioned_images_index: dict):
    sheetnames = pd.read_excel(excel_file,
                                sheet_name = None,
                                engine = 'openpyxl',
                                na_filter=False).keys()
    pandas_data = { }
    for sheetname in sheetnames:
        if sheetname.lower() == 'single':
            sheetname = sheetname.lower()
        else:
            sheetname = sheetname.upper()
        pandas_data[sheetname] = pd.read_excel(excel_file,
                                              sheet_name = sheetname,
                                              engine = 'openpyxl',
                                              na_filter = False,
                                              dtype=str)
        pandas_data[sheetname] = pandas_data[sheetname].loc[:, ~pandas_data[sheetname].columns.str.contains('^Unnamed')].to_dict()
      
        for idx, f in pandas_data[sheetname]['filename'].items():
            #print(idx,f, pandas_data[sheetname]['image_id'][idx], partitioned_images_index[f])
            pandas_data[sheetname]['image_id'][idx] = partitioned_images_index[f]

    with pd.ExcelWriter(excel_file) as writer:
        for sheetname, data in pandas_data.items():
            pd_output = pd.DataFrame(data)
            pd_output.to_excel(writer, sheet_name = sheetname, engine='openpyxl', index=False)

def check_filename_billing(list_path_excel:str) -> dict:
    return_output = {}
    for path in list_path_excel:
        df_dict = pd.read_excel(path, sheet_name=None)

        for sheetname in df_dict.keys():
            if sheetname.lower() == 'single':
                df_single = df_dict.get(sheetname)
            elif sheetname.lower() == 'billingitems':
                df_billitem = df_dict.get(sheetname)
            
        if df_billitem is None:
            continue

        for _, row in df_billitem.iterrows():
            if row['filename'] not in df_single['filename'].tolist():
                if path in return_output:
                    if row['filename'] not in return_output[path]:
                        return_output[path].append(row['filename'])
                else:
                    return_output[path] = [row['filename']]
    return return_output        

def error_to_excel(filename, errors):
    df_dict = {}
    for d in errors:                       
        error_type = d['error_type']
        messages   = d['messages']
        if error_type not in df_dict:
            df_dict[error_type] = {'messages':list()}
        df_dict[error_type]['messages'].append(messages)
    
    with pd.ExcelWriter(filename) as writer:
        for sheetname, data in df_dict.items():
            pd_output = pd.DataFrame(data)
            pd_output.to_excel(writer, 
                                sheet_name = sheetname, 
                                engine='openpyxl', index=False)
    


def main(args):
    # get all arguments
    project_code = args.project_code
    input_path = args.input_path
    container_string = args.container_string
    num_partition = args.num_partition
    partition_idx = [int(_idx) for _idx in args.partition_idx.split(',')] 
    az_crediential = args.az_crediential
    benchmark     = args.benchmark
    

    if benchmark == False:
        main_folder_az = '/'.join(['research','data',project_code])
        output_json_name = f'{project_code}.json'
        output_json_folder_az = '/'.join(['research','data','label_config'])
        excel_folder_az = '/'.join(['research','data',project_code,'excel'])
        image_folder_az = '/'.join(['research','data',project_code,'images'])
    else:
        main_folder_az = '/'.join(['benchmark','data',project_code])
        output_json_name = f'{project_code}.json'
        output_json_folder_az = '/'.join(['benchmark','data','label_config'])
        excel_folder_az = '/'.join(['benchmark','data',project_code,'excel'])
        image_folder_az = '/'.join(['benchmark','data',project_code,'images'])

   
    
    check_file_result = check_file_image(main_folder_az, folder=input_path,header_name="filename")
    list_file_name_image_check_local,\
        list_path_excel,\
        list_file_name_image_check_blob,\
        list_path_blob_excel,\
        error_check, \
        images_no_found, \
        printoutputs, format_errors = check_file_result \
        
    # assertion
    assert os.path.exists('/'.join([input_path,'images'])) ,'Image folder not found'
    assert os.path.exists('/'.join([input_path,'excel'])) ,'Excel folder not found'
    #assert error_check == False,'Some images were not found in the folder.'
    #sheet_filename_checked = check_filename_billing(list_path_excel)
    #assert sheet_filename_checked == {}, f"File name in 'BILLINGITEM' sheet not in 'single' sheet:\n{sheet_filename_checked}"
    
    local_image_path = os.path.join(input_path, 'images')
    partition_folder = os.path.join(input_path, "partition")
    save_error_excel = os.path.join(input_path, f'{project_code}_errors.xlsx')
    
    partitioned_excel_path_list, partion_errors, format_partion_errors = partition_excel(list_path_excel, 
                                                    num_partition, 
                                                    project_code, 
                                                    input_path,
                                                    images_no_found)


    print("All partitioned excel files:", partitioned_excel_path_list)

    error_to_excel(filename = save_error_excel, errors = format_errors+format_partion_errors)

    # choose one of partitioned file
    selected_excels = list()
    print(partition_idx)
    max_partition = len(partitioned_excel_path_list)
    for _idx in partition_idx:
        if _idx < max_partition:
            selected_excels += partitioned_excel_path_list[_idx:_idx+1]

    
    print(f"Selected partitioned excel files: {selected_excels}")


    partitioned_images_list = list()
    partitioned_excel_list  = list()
    for excel_path in selected_excels:
        excel_fullpath = os.path.join(partition_folder, excel_path)
        partitioned_excel_list.append(excel_fullpath)
        partitioned_images_list += get_imagefiles_from_excel(excel_fullpath, local_image_path)
   
    # Clean duplicate images
    partitioned_images_list = sorted(list(set(partitioned_images_list)))


    # Upload image and excel
    print('-'*10,'Upload image','-'*10)
    folder_blob_list = [image_folder_az]*len(partitioned_images_list)
    status = thread_upload(container_string, folder_blob_list, partitioned_images_list)
    #print('upload image status', status)
    
    print('-'*10,'Upload excel','-'*10)
    folder_blob_list = [excel_folder_az]*len(partitioned_excel_list)
    uploaded_excel = thread_upload(container_string, folder_blob_list, partitioned_excel_list)
    #print('upload excel status', status)

    if len(printoutputs) > 0 or len(partion_errors):
        print(f'---------- WARNING -----------')
        for printout in printoutputs:
            print(printout)
        for printout in partion_errors:
            print(printout)
        print(f'------------------------------')
    
    # upload json
    json_output = {
        'project_code':project_code,
        'excel_path_az':get_az_endpoint(az_path = uploaded_excel),
        'image_path_az':image_folder_az,
        'container_string':container_string
    }
    save_json(json_output, output_json_name)
    print('-'*10,'Upload json','-'*10)
    status, url = upload_to_blob(container_string, output_json_folder_az, output_json_name)
    print("status:",status,"url:",url)
    print('-'*20)

    print(json_output)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Upload excel and image with thread to Azure Blob storage')

    parser.add_argument('--project_code', type=str, default='None', help='')
    parser.add_argument('--input_path', type=str, default='None', help='')
    parser.add_argument('--container_string', type=str, default='None', help='')
    parser.add_argument('--num_partition', type=int, default=10, help='number of partition from each excel file to use for dividing')
    parser.add_argument('--partition_idx', type=str,  
                            default=0, help='index of partition to upload (start from 0)')
    parser.add_argument('--az_crediential', type=str, default='None', help='')
    parser.add_argument('--benchmark',  action='store_true')
    args = parser.parse_args()
    partition_idx = [int(partition_idx) for partition_idx in args.partition_idx.split(',')] 
    for _idx in partition_idx:
        assert _idx in range(args.num_partition), f"partition_idx [{partition_idx}] not in {range(args.num_partition)}"

    main(args)