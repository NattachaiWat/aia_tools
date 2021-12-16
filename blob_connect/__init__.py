import os
import logging
log_file = os.getenv('log_file', 'logging.log') 
logging.basicConfig(filename = log_file, 
                      filemode = 'w+', 
                      level = logging.INFO, 
                      format = '%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s')

from urllib.parse import urlparse
from datetime import datetime, timedelta

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions

KEY_VAULT_ACCOUNT = os.getenv('KEY_VAULT_ACCOUNT')
BLOB_ACCOUNT = os.getenv("SECRET_1")
BLOB_KEY = os.getenv("SECRET_2")

CLIENT_ID = os.getenv("CLIENT_ID")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")

CONNECTION_STRING = os.getenv('CONNECTION_STRING', None)

class AzureBlob:
    def __init__(self, conn_str = CONNECTION_STRING):
        if conn_str is None:
            blob_account = os.getenv("BLOB_ACCOUNT")
            blob_key = os.getenv("BLOB_KEY")
            if CLIENT_ID is not None:
                default_credential = DefaultAzureCredential(managed_identity_client_id=CLIENT_ID)
                account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
                self._BLOB_CLIENT = BlobServiceClient(account_url=account_url, credential=default_credential)
                return
            elif blob_account is None or blob_key is None:
                credential = DefaultAzureCredential()
                vault_url = f'https://{KEY_VAULT_ACCOUNT}.vault.azure.net'
                vault_client = SecretClient(vault_url=vault_url, credential=credential)
                blob_account = vault_client.get_secret(BLOB_ACCOUNT).value
                blob_key = vault_client.get_secret(BLOB_KEY).value
                os.environ["BLOB_ACCOUNT"] = blob_account
                os.environ["BLOB_KEY"] = blob_key

            self.account_name = blob_account
            self.account_key = blob_key + '=='
            self.conn_str = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};" \
                            f"AccountKey={self.account_key};EndpointSuffix=core.windows.net"
            self._BLOB_CLIENT = BlobServiceClient.from_connection_string(self.conn_str)
        elif isinstance(conn_str, str):
            self.conn_str = conn_str
            self._BLOB_CLIENT = BlobServiceClient.from_connection_string(self.conn_str)

    def upload(self, container, blob, path_file):
        # dir_blob = os.path.dirname(path_file)
        # if len(dir_blob) != 0:
            # blob = path_file.replace(dir_blob, blob)
        # else:
            # blob = path_file

        basename = os.path.basename(path_file)
        blob = '/'.join([blob,basename])

        blob_client = self._BLOB_CLIENT.get_blob_client(container=container, blob=blob)
        # print("\nUploading to Azure Storage as blob:\n\t" + path_file)
        with open(path_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            # print(blob_client.url)
            return True, blob_client.url

    def download(self, container, blob_url, local_path):
        url = urlparse(blob_url, allow_fragments=False)
        blob_url = url.path.replace(container, "", 1)
        blob_url = blob_url.replace("//", "")
        blob_container = self._BLOB_CLIENT.get_container_client(container)
        try:
            file = blob_container.get_blob_client(blob_url).download_blob().readall()
            self._save_blob(local_path, file)
            return True, local_path
        except ResourceNotFoundError:
            raise ResourceNotFoundError(f"The specified {blob_url} does not exist.")

    def copy(self, source_container, source_blob, target_container, target_file_path, delete=False):
        copied_blob = self._BLOB_CLIENT.get_blob_client(target_container, target_file_path)
        copy_properties = copied_blob.start_copy_from_url(source_blob)

        if delete:
            # If you would like to delete the source file
            remove_blob = self._BLOB_CLIENT.get_blob_client(source_container, source_blob)
            remove_blob.delete_blob()

        return copy_properties

    @staticmethod
    def _save_blob(local_path, file_content):
        # for nested blobs, create local path as well!
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with open(local_path, "wb") as file:
            file.write(file_content)

    def upload_sas(self, container, blob, path_file, expiry_minute):
        dir_blob = os.path.dirname(path_file)
        if len(dir_blob) != 0:
            blob = path_file.replace(dir_blob, blob)
        else:
            blob = path_file
        _azure_client_sas = BlobServiceClient.from_connection_string(self.conn_str,
                                                                     credential=self._get_sas_token(expiry_minute))
        blob_client = _azure_client_sas.get_blob_client(container=container, blob=blob)
        with open(path_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            return True, blob_client.url

    def _get_sas_token(self, expiry_minute):
        sas_token = generate_account_sas(
            account_name=self.account_name,
            account_key=self.account_key + '==',
            resource_types=ResourceTypes(object=True),
            permission=AccountSasPermissions(read=True, write=True),
            expiry=datetime.utcnow() + timedelta(minutes=expiry_minute)
        )
        return sas_token


def download_from_blob(container, blob_url, folder):
    """

    Args:
        container: container on azure blob storage
        blob_url: blob url to download file from container
        folder: local folder to save downloaded

    Returns:
        success: status
        path_file: local path on storage

    """
    azure_blob = AzureBlob()
    success, path_file = azure_blob.download(container, blob_url, folder)
    return success, path_file


def upload_to_blob(container, blob, path_file):
    """

    Args:
        container: container on azure blob storage
        blob: destination on azure blob storage for upload
        path_file: path to local file for upload to azure blob

    Returns:
        success: status
        blob_url: url for access to file on azure blob storage

    """
    azure_blob = AzureBlob()
    success, blob_url = azure_blob.upload(container, blob, path_file)
    return success, blob_url


def upload_to_blob_sas(container, blob, path_file, expiry_minute=5):
    """
    Args:
        container: container on azure blob storage
        blob: destination on azure blob storage for upload
        path_file: path to local file for upload to azure blob

    Returns:
        success: status
        blob_url: url for access to file on azure blob storage

    """
    azure_blob = AzureBlob()
    success, blob_temp_url = azure_blob.upload_sas(container, blob, path_file, expiry_minute)
    return success, blob_temp_url


def copy_blob_to_another_blob(source_container, source_blob, target_container, target_file_path, delete=False):
    azure_blob = AzureBlob()
    copy_properties = azure_blob.copy(source_container, source_blob, target_container, target_file_path, delete)
    return copy_properties


if __name__ == '__main__':
    container = "stbifrostdatadev001"
    local_file_name = "/Users/jetsukda/Desktop/bifrost-z01/hippo_tools/connector/blob_connect/texts/_test_file_upload.txt"
    # azure_blob = AzureBlob()
    # _, url = azure_blob.upload(container_name, "hippo", local_file_name)
    # print(f"url: {url}")
    _, blob_url_ = upload_to_blob(container="stbifrostdatadev001", blob="hippo", path_file=local_file_name)
    print("\nBlob URL:\n\t" + blob_url_)

    folder_, image_name = os.path.split(local_file_name)  # ('folder1/folder2/', 'xxx.jpg')
    folder_ = folder_.replace('/', '_')
    local_image_name = f"{folder_}&|{image_name}"  # 'folder1_folder2_&|xxx.jpg'
    local_image_path = f"download/{local_image_name}"
    _, local_path_file = download_from_blob(container="stbifrostdatadev001", blob_url=blob_url_, folder=local_image_path)
    print("\nLocal path:\n\t" + local_path_file)