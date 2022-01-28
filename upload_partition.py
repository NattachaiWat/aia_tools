import os
import sys
import os.path as osp
from typing import List, Tuple
import json
import numpy as np
import pandas as pd
from multiprocessing.pool import ThreadPool

from blob_connect import upload_to_blob

NUM_PROCESSORS = 10

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

    for path_file_read in list_path_excel:
        result = pd.read_excel(path_file_read, index_col=None, header=None)
        rows,columns = result.shape
        i_header_name = None
        for i_columns in range(columns):
            if result[i_columns][0] == header_name:
                i_header_name = i_columns
                break

        list_image_name_excel = list(result[i_header_name][1:])
        num_filename_excel += len(list_image_name_excel)
        
        
        for file_name_image in list_image_name_excel:
            if file_name_image in list_file_name_image:
                value_path_image_local = list_path_file_image[list_file_name_image.index(file_name_image)]
                value_path_image_blob = list_path_blob_image[list_file_name_image.index(file_name_image)]
                list_file_name_image_check_local.append(value_path_image_local)
                list_file_name_image_check_blob.append(value_path_image_blob)
            else:
                if str(file_name_image).lower() != 'nan' and \
                    str(file_name_image).strip() != '' and \
                    str(file_name_image).lower() != 'none' and \
                    file_name_image is not None:

                    print(file_name_image,"is not in folder",folder)
                    error_check = True
    
    return list_file_name_image_check_local,list_path_excel,list_file_name_image_check_blob,list_path_blob_excel,error_check

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

    with ThreadPool(processes=num_process) as pool:

        result = pool.starmap(upload_to_blob, list_work)

    return result

def get_split_list(lst, size):
    return [list(i) for i in np.array_split(lst, size)]

def partition_billitem(list_path_excel:List[str], num_partition:int) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    single_df_list = [[] for _ in range(num_partition)]
    billitem_df_list = [[] for _ in range(num_partition)]
    for i, path in enumerate(list_path_excel):
        df_dict = pd.read_excel(path, sheet_name=None)
        df_single = df_dict.get('single')
        df_billitem = df_dict.get('BILLINGITEMS')
        
        idx_list = get_split_list(list(df_single.image_id.values), num_partition)
        for j, idx in enumerate(idx_list):
            temp_df_single = df_single.iloc[idx]
            image_idx = temp_df_single.image_id.values
            single_df_list[j].append(temp_df_single)
            
            temp_df_billingitem = df_billitem[df_billitem['image_id'].isin(image_idx)]
            billitem_df_list[j].append(temp_df_billingitem)
    
    df_partition = []
    for i, (singel_part, billitem_part) in enumerate(zip(single_df_list, billitem_df_list)):
        df_single = pd.concat(singel_part, ignore_index=True)
        df_billitem = pd.concat(billitem_part, ignore_index=True)
        
        df_single.image_id = range(df_single.shape[0])
        df_billitem.image_id = df_billitem["filename"].apply(lambda x: df_single[df_single["filename"] == x]["image_id"].index[0])
        
        df_partition.append((df_single, df_billitem))
        
    return df_partition

def partition_single(list_path_excel:List[str], num_partition:int) -> List[pd.DataFrame]:
    single_df_list = [[] for _ in range(num_partition)]
    for i, path in enumerate(list_path_excel):
        df_dict = pd.read_excel(path, sheet_name=None)
        df_single = df_dict.get('single')
        
        idx_list = get_split_list(list(df_single.image_id.values), num_partition)
        for j, idx in enumerate(idx_list):
            temp_df_single = df_single.iloc[idx]
            single_df_list[j].append(temp_df_single)
    
    df_partition = []
    for i, singel_part in enumerate(single_df_list):
        df_single = pd.concat(singel_part, ignore_index=True)
        df_single.image_id = range(df_single.shape[0])
        
        df_partition.append(df_single)
        
    return df_partition

def partition_excel(list_path_excel:List[str], num_partition:int, project_code:str, input_folder:str) -> str:
    """
    แบ่งแต่ละ excel ไฟล์ที่อยู่ใน list_path_excel เป็นส่วนๆเป็นจำนวน num_partition ส่วน
    แล้วเอาส่วนที่ i ของแต่ละไฟล์มา merge รวมกันแล้ว save excel แต่ละส่วน
    Args:
        list_path_excel (List[str]): [description]
        num_partition (int): [description]
    """
    temp_dict = pd.read_excel(list_path_excel[0], sheet_name=None)
    
    save_folder = osp.join(input_folder, "partition")
    if not osp.exists(save_folder):
        os.makedirs(save_folder)
    
    excel_path_list = []
    if temp_dict.get('BILLINGITEMS') is None:
        df_partition = partition_single(list_path_excel, num_partition)
        for i, df_part in enumerate(df_partition):
            save_path = f"{project_code}_{i}of{num_partition}.xlsx"
            excel_path_list.append(save_path)
            with pd.ExcelWriter(osp.join(save_folder, save_path)) as writer:
                df_part.to_excel(writer, sheet_name='single', engine='openpyxl', index=False)
    else:
        df_partition = partition_billitem(list_path_excel, num_partition)
        for i, (df_single, df_billitem) in enumerate(df_partition):
            save_path = f"{project_code}_{i}of{num_partition}.xlsx"
            excel_path_list.append(save_path)
            with pd.ExcelWriter(osp.join(save_folder, save_path)) as writer:
                df_single.to_excel(writer, sheet_name='single', engine='openpyxl', index=False)
                df_billitem.to_excel(writer, sheet_name='BILLINGITEMS', engine='openpyxl', index=False)
    
    return excel_path_list


def main(args):
    # get all arguments
    project_code = args.project_code
    input_path = args.input_path
    container_string = args.container_string
    num_partition = args.num_partition
    az_crediential = args.az_crediential
    
    main_folder_az = f"research/data/{project_code}"
    output_json_name = f'{project_code}.json'
    output_json_folder_az = f"research'data/label_config"
    
    check_file_result = check_file_image(main_folder_az, folder=input_path,header_name="filename")
    list_file_name_image_check_local,\
        list_path_excel,\
        list_file_name_image_check_blob,\
        list_path_blob_excel,\
        error_check = check_file_result
    
    # assertion
    assert os.path.exists('/'.join([input_path,'images'])) ,'Image folder not found'
    assert os.path.exists('/'.join([input_path,'excel'])) ,'Excel folder not found'
    assert error_check == False,'Some images were not found in the folder.'
    
    partitioned_excel_path_list = partition_excel(list_path_excel, num_partition, project_code, input_path)
    print(partitioned_excel_path_list)

    # Upload image and excel
    print('-'*10,'Upload image','-'*10)
    folder_blob_list = [osp.dirname(path) for path in list_file_name_image_check_blob]
    thread_upload(container_string, folder_blob_list, list_file_name_image_check_local)
    
    print('-'*10,'Upload excel','-'*10)
    folder_blob_list = [osp.dirname(path) for path in list_path_blob_excel]
    thread_upload(container_string, folder_blob_list, partitioned_excel_path_list)
    
    # upload json
    image_path_az = osp.dirname(list_file_name_image_check_blob[0])
    json_output = {
        'project_code':project_code,
        'excel_path_az':list_path_blob_excel,
        'image_path_az':image_path_az,
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
    parser.add_argument('--az_crediential', type=str, default='None', help='')
    args = parser.parse_args()

    main(args)