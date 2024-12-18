pip install smbprotocol
pip install azure-storage-blob

dbutils.library.restartPython()

# dbutils.widgets.removeAll()

import smbclient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

dbutils.widgets.text('SMBClientUsername', 'usiphdireporting@bp.com')
dbutils.widgets.text('SMBClientPassword', 'USIPHDIREPORTING-PWD-KV')
dbutils.widgets.text('keyVaultName', 'ZNEUDL1P48DEF-kvl01') # "ZNEUDL1P40INGing-kvl00" or "ZNEUDL1P48DEF-kvl01"

dbutils.widgets.text('sourceFolderRootPath', 'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/_Hub/Catchment_Area/Wells/SilvertipSA001')

dbutils.widgets.text('tgtStorageAccount', 'zneudl1p40ingstor01') # "zneusu5p82cggdata" or "zneudl1p40ingstor01"
dbutils.widgets.text('tgtStorageAccountKeyName', 'accessKey-PIM-zneudl1p40ingstor01') # "accessKey-CGG-zneusu5p82cggdata" or "accessKey-PIM-zneudl1p40ingstor01"
dbutils.widgets.text('tgtContainer', 'unstructured-data') # "bptocgg"
dbutils.widgets.text('tgtFolderRootPath', 'OBO_Great_White_Data/NetApp/GW_Area/_Hub/Catchment_Area/Wells/SilvertipSA001/')

%run "../ModuleFunctions"

# Get widget values
SMB_CLIENT_NAME = dbutils.widgets.get('SMBClientUsername')
SMB_CLIENT_PWD_NAME = dbutils.widgets.get('SMBClientPassword')
KEY_VAULT = dbutils.widgets.get('keyVaultName')
TARGET_STORAGE = dbutils.widgets.get('tgtStorageAccount')
TARGET_STORAGE_KEY_NAME = dbutils.widgets.get('tgtStorageAccountKeyName')
TARGET_CONTAINER = dbutils.widgets.get('tgtContainer')
SOURCE_FOLDER = Utility.remove_leading_and_trailing_slashes(dbutils.widgets.get('sourceFolderRootPath')) # NetApp source folder location being copied, e.g. aadsmbuswp-895a.bp1.ad.bp.com/usw1/hou_group_005/NAX/
TARGET_FOLDER = Utility.remove_leading_and_trailing_slashes(dbutils.widgets.get('tgtFolderRootPath')) # Azure Blob Storage folder path where the data is to land, e.g. Nakika_Data/NetApp/NK_ILX/
SMB_CLIENT_PWD = dbutils.secrets.get(KEY_VAULT, SMB_CLIENT_PWD_NAME)

# Configure target Azure Blob Storage Account
TARGET_STORAGE_ACCESS_KEY = dbutils.secrets.get(scope = KEY_VAULT, key = TARGET_STORAGE_KEY_NAME)
BLOB_SERVICE_CLIENT = BlobServiceClient(account_url = f'https://{TARGET_STORAGE}.blob.core.windows.net', credential = TARGET_STORAGE_ACCESS_KEY)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(TARGET_CONTAINER)
# smb_file_path = 'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/_Hub/Catchment_Area/Wells/SilvertipSA001/Visio-Silvertip SA-001 Completion Diagram-as built.ZIP'
smb_file_path = 'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/_Hub/Team_Planning/05.12_Partner_Agrmt/'

root_relative_path = Utility.get_root_relative_path(smb_file_path, SOURCE_FOLDER)
azure_blob_name = f'{TARGET_FOLDER}/{root_relative_path}'
tgt = f'https://{TARGET_STORAGE}.blob.core.windows.net/{TARGET_CONTAINER}/{azure_blob_name}'
items = smbclient.listdir(smb_file_path, username = SMB_CLIENT_NAME, password = SMB_CLIENT_PWD)


for i in items:
  item_path = os.path.join(smb_file_path, i)
  print(item_path)
smb_file_path = 'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/_Hub/Team_Planning/05.12_Partner_Agrmt/AlaminosJOA.pdf'
root_relative_path = Utility.get_root_relative_path(smb_file_path, SOURCE_FOLDER)
azure_blob_name = f'{TARGET_FOLDER}/{root_relative_path}'
tgt = f'https://{TARGET_STORAGE}.blob.core.windows.net/{TARGET_CONTAINER}/{azure_blob_name}'
tgt
with smbclient.open_file(smb_file_path, mode='rb', username=SMB_CLIENT_NAME, password=SMB_CLIENT_PWD) as file:
    uploadSuccess = Utility.upload_blob_to_container(CONTAINER_CLIENT, azure_blob_name, file.read())
    if uploadSuccess:
        print(f"✅ {tgt}")
import hashlib


def calculate_md5(file_obj):
    """This calculates the MD5 hash of a file by reading it in chunks (to handle large files without consuming too much memory) and returns the hash."""
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: file_obj.read(4096), b""):
      hash_md5.update(chunk)
    file_obj.seek(0)  # Reset file pointer after reading for further operations
    return hash_md5.hexdigest()
  
with smbclient.open_file(smb_file_path, mode='rb', username=SMB_CLIENT_NAME, password=SMB_CLIENT_PWD) as source_file:
    # Step 1: Calculate MD5 hash of the source file
    source_md5 = calculate_md5(source_file)
    
    # Step 2: Upload the file to Azure Blob Storage
    uploadSuccess = Utility.upload_blob_to_container(CONTAINER_CLIENT, azure_blob_name, source_file.read())
    
    if uploadSuccess:
        print(f"✅ File uploaded to {tgt}")
        
        # Step 3: Download the file from Azure Blob Storage to compare
        blob_client = CONTAINER_CLIENT.get_blob_client(blob=azure_blob_name)
        downloaded_blob = blob_client.download_blob().readall()
        
        # Step 4: Calculate MD5 hash of the downloaded file
        downloaded_md5 = hashlib.md5(downloaded_blob).hexdigest()
        
        # Step 5: Compare MD5 hashes
        if source_md5 == downloaded_md5:
            print("✅ MD5 verification successful: File was copied without corruption.")
        else:
            print("❌ MD5 verification failed: The file may be corrupted.")
