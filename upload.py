from blob_connect import upload_to_blob,download_from_blob
import pandas as pd
import os
import argparse
import json

parser = argparse.ArgumentParser(description='Upload image to azure blob storage.')

parser.add_argument('--project_code', type=str, default='None', help='')
parser.add_argument('--input_path', type=str, default='None', help='')
parser.add_argument('--container_string', type=str, default='None', help='')
parser.add_argument('--az_crediential', type=str, default='None', help='')

args = parser.parse_args()

global project_code,input_path,container_string,az_crediential
project_code = args.project_code
input_path = args.input_path
container_string = args.container_string
az_crediential = args.az_crediential
print("project_code:",project_code,type(project_code))
print("input_path:",input_path,type(input_path))
print("container_string:",container_string,type(container_string))
print("az_crediential:",az_crediential,type(az_crediential))

main_folder_az = f'research/data/{project_code}'
output_json_name = f'{project_code}.json'
output_json_folder_az = f'research/data/label_config'

def save_json(json_data, path_output_json):
    with open(path_output_json, 'w', encoding='utf8') as json_file:
        json.dump(json_data.copy(), json_file, ensure_ascii=False, indent=4)

def find_file(folder=None,type_file=["xlsx"],remove_main_path=False):
    list_path_blob_file = []
    list_path_file = []
    list_file_name = []
    if folder is None:
        return list_path_file
    
    folder_find_images = os.path.join(folder,'images')
    folder_find_excel = os.path.join(folder,'excel')

    for main_folder,sub_folder,list_file in os.walk(folder):
        if str(main_folder).find(folder_find_images) == 0 or (str(main_folder).find(folder_find_excel) == 0 and "xlsx" in type_file):
            for file in list_file:
                type_file_split = str(os.path.basename(file)).split('.')[-1]
                if str(type_file_split).lower() in str(type_file).lower():
                    path_blob = main_folder
                    if remove_main_path == True:
                        path_blob = str(main_folder)[len(folder)+1:]
                    list_path_blob_file.append(os.path.join(main_folder_az,path_blob,file))
                    list_path_file.append(os.path.join(main_folder,file))
                    list_file_name.append(os.path.basename(file))

    return list_path_file,list_file_name,list_path_blob_file

def check_file_image(folder=None,header_name="filename"):
    list_path_file_image,list_file_name_image,list_path_blob_image = find_file(folder=folder,type_file=['tiff','tif', 'jpg', 'jpeg', 'bmp', 'png', 'pdf'],remove_main_path=True)
    list_path_excel,_,list_path_blob_excel = find_file(folder=folder,type_file=["xlsx"],remove_main_path=True)
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

def upload_file_to_blob(check_image=True):
    print('')
    assert os.path.exists(os.path.join(input_path,'images')) ,'Image folder not found'
    assert os.path.exists(os.path.join(input_path,'excel')) ,'Excel folder not found'

    list_file_name_image_check_local,\
        list_path_excel,\
            list_file_name_image_check_blob,\
                list_path_blob_excel,\
                    error_check = check_file_image(folder=input_path,header_name="filename")

    assert len(list_path_excel) > 0,'Excel file not found.'
    assert len(list_file_name_image_check_local) > 0,'Image file not found.'
    if check_image == True:
        assert error_check == False,'Some images were not found in the folder.'
             
    print('-'*10,'Upload image','-'*10)
    for i, local_image_path in enumerate(list_file_name_image_check_local):
        folder_blob = '/'.join(str(list_file_name_image_check_blob[i]).split('/')[:-1])
        status, url = upload_to_blob(container_string, folder_blob, local_image_path)
        print(i+1,'/',len(list_file_name_image_check_local),"status:",status,"url:",url)
    print('-'*20)
    
    print('-'*10,'Upload excel','-'*10)
    for i, local_excel_path in enumerate(list_path_excel):
        folder_blob = '/'.join(str(list_path_blob_excel[i]).split('/')[:-1])
        status, url = upload_to_blob(container_string, folder_blob, local_excel_path)
        print(i+1,'/',len(list_path_excel),"status:",status,"url:",url)
    print('-'*20)
    
    json_output = {
        'project_code':project_code,
        'excel_path_az':list_path_blob_excel,
        'image_path_az':list_file_name_image_check_blob,
        'container_string':container_string
    }

    save_json(json_output, output_json_name)

    print('-'*10,'Upload json','-'*10)
    status, url = upload_to_blob(container_string, output_json_folder_az, output_json_name)
    print("status:",status,"url:",url)
    print('-'*20)

    print(json_output)

if __name__ == '__main__':
    upload_file_to_blob(check_image=False)