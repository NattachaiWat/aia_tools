import os
import sys
import traceback
import pandas as pd
from pathlib import Path
from connectors.connectors_email import send_email

if os.getenv('COSMOS_CLIENT_ID'):
    from connectors.mongodb import DB
else:
    from connectors.logmongo import DB

def get_db(project_code):
    ## get DB name from list
    map_project_db = {
        "AIA001" : "zcheck_aia_product_claim_id_card_001",
        "AIA002" : "zcheck_aia_product_claim_group_privilege_card_002",
        "AIA003" : "zcheck_aia_product_claim_ipd_claim_form_003",
        "AIA004" : "zcheck_aia_product_claim_opd_claim_form_004",
        "AIA005" : "zcheck_aia_product_claim_dental_claim_form_005",
        "AIA006" : "zcheck_aia_product_claim_submission_form_006",
        "AIA007" : "zcheck_aia_product_claim_medical_certificate_007",
        "AIA008" : "zcheck_aia_product_claim_original_billing_008",
        "AIA009" : "zcheck_aia_product_claim_warnimg_sheet_009",
        "AIA010" : "zcheck_aia_product_nb_bookbank_010",
        "AIA011" : "zcheck_aia_product_nb_birth_certificate_011",
        "AIA012" : "zcheck_aia_product_nb_id_card_012",
        "AIA013" : "zcheck_aia_product_nb_passport_013",
        "AIA014" : "zcheck_aia_product_claim_medical_certificate_014",
        "AIA015" : "zcheck_aia_product_claim_original_billing_015",
        "AIA016" : "zcheck_aia_product_nb_bookbank_016",
    }
    if project_code in map_project_db:
        db_name = map_project_db[project_code]
    else: db_name = "No-Project"
    return db_name


def query_ocr_result(db_name):
    ## query data
    database = DB(db_name)
    if os.getenv('COSMOS_CLIENT_ID'):
        print("msi")
        mydoc = database.find_all_df(collection="ocr_result")
    else:
        print("connection_str")
        mydoc = database.find_all(collection="ocr_result")
    print(mydoc)
    return mydoc

def query_ocr_result_mergeDB(db_name):
    ## query data
    database = DB("zcheck_qc")
    print("db_name = ", db_name)
    cond = {"db_name":db_name}
    if os.getenv('COSMOS_CLIENT_ID'):
        print("msi")
        mydoc = database.query_df(collection="ocr_result", cond = cond)
        # mydoc = database.find_all_df(collection="ocr_result")
    else:
        print("connection_str")
        mydoc = database.query(collection="ocr_result", cond = cond)
        # mydoc = database.find_all(collection="ocr_result")
    print(mydoc)
    return mydoc

def prepare_data(input_data):
    ## pack csv data from query data
    Report_data = []
    if len(input_data) == 0:
        csv_path = "No-Data"
        return csv_path
    else:
        try:
            for results in input_data['ocr_result']:
                Report = []
                id = results['_id']
                for i in range(len(results['data'])):
                    dict_output = []
                    data_dict = results['data'][i]['information']
                    for key, value in data_dict.items() :
                        data_output = []
                        if 'BILLINGITEMS' in key:
                            for j in range(len(value)):
                                for key_item, value_item in value[j].items():
                                    data_item = []
                                    item_filed = key_item
                                    item_text = value_item['text']
                                    item_text_type = value_item['text_type']
                                    item_confident = value_item['confident']
                                    data_item = [id, item_filed, item_text, item_text_type, item_confident]
                                    dict_output.append(data_item)
                        else:
                            field = key
                            text = value['text']
                            text_type = value['text_type']
                            confident = value['confident']
                            data_output = [id, field, text, text_type, confident]
                            dict_output.append(data_output)
                    Report.extend(dict_output)
                Report_data.extend(Report)
            fields = ['id', 'field', 'text', 'text_type', 'confident' ]
            df = pd.DataFrame(Report_data)
            folder_name = 'output/'
            full_path = os.path.abspath(folder_name)
            Path(full_path).mkdir(parents=True, exist_ok=True)
            df.to_csv('{0}/{1}.csv'.format(full_path, database_name), index=False, header= fields ,sep=",")
            csv_path = '{0}/{1}.csv'.format(full_path, database_name)
            
        except Exception as err:
            print(err)
            csv_path = "No-Data"
            raise Exception(traceback.format_exc())

        return csv_path

if __name__ == "__main__":

    # Project Code:
    project_code_list = os.getenv("PROJECT_CODE")
    assert project_code_list is not None, "PROJECT_CODE does not define!"
    project_code_list = project_code_list.split(',')
    print(type(project_code_list))
    print(project_code_list)
    for project_code in project_code_list:
        print(project_code)
        ## get db name from project code
        database_name = get_db(project_code)
        print("database_name = ",database_name)
        if "No-Project" not in database_name:
            ## query data from mongoDB
            # data_mongo = query_ocr_result(database_name)
            data_mongo = query_ocr_result_mergeDB(database_name)
            # ## pack csv data
            csv_path = prepare_data(data_mongo)
        else: csv_path = "No-Data"
        
        ##prepare send email
        sender_username = os.getenv('SENDER_EMAIL','')
        sender_password = os.getenv('SENDER_PASSWORD','')
        receiver_emails = os.getenv('RECEIVER_EMAIL', '')
        smtp_port  = os.getenv('smtp_port', 587)
        useconnect = True if os.getenv('smtp_useconnect', 'false') == 'true' else False
        receiver_emails = receiver_emails.split(',')

        host = os.getenv('smtp_email')
        assert len(receiver_emails) != 0, 'please define RECEIVER_EMAIL = '
        assert host is not None, 'please define host = ?'

        print(f'Send email to {receiver_emails}')
        print(f'host: {host}:{smtp_port}')
        print(f'Connect SMTP bool: {useconnect}')
        if "No-Data" in csv_path:
            try:
                send_email( 
                            targets = receiver_emails, 
                            username = sender_username,
                            password = sender_password,
                            host = host, 
                            port = smtp_port,
                            SUBJECT = 'No Data in  {0}'.format(project_code),
                            body = "Project {0} has no data or wrong Project Code".format(project_code), 
                            useconnect = useconnect,
                            zipfile = None
                        )
                print('Send Email: Pass')
                # sys.exit(0)
            except Exception as err:
                print(err)
                raise Exception(traceback.format_exc())
        else:
            try:
                send_email( 
                            targets = receiver_emails, 
                            username = sender_username,
                            password = sender_password,
                            host = host, 
                            port = smtp_port,
                            SUBJECT = 'CSV for {0}'.format(project_code),
                            body = 'CSV for {0}'.format(project_code), 
                            useconnect = useconnect,
                            zipfile = csv_path
                        )
                print('Send Email: Pass')
                # sys.exit(0)
            except Exception as err:
                print(err)
                raise Exception(traceback.format_exc())
    sys.exit(0)