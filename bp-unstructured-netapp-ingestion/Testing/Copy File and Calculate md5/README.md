# SMB to Azure Blob Storage File Transfer

This program transfers files from a NetApp SMB (Server Message Block) server to Azure Blob Storage, ensuring file integrity by calculating and comparing MD5 hashes before and after the transfer.

---

## **Features**
- **File Transfer**: Copies files from an SMB server to Azure Blob Storage.
- **File Integrity Check**: Verifies the uploaded files using MD5 hashing to ensure no corruption occurred during the transfer.
- **Customizable Paths**: Configurable source and target folder paths.
- **Scalability**: Processes large files efficiently using chunked reading for minimal memory usage.

---

## **Steps Performed**

### 1. **Installation and Initialization**
The following Python packages are required:
- `smbprotocol`: For interacting with SMB servers.
- `azure-storage-blob`: For interacting with Azure Blob Storage.

```bash
pip install smbprotocol
pip install azure-storage-blob
```

Restart the Python environment:
```python
dbutils.library.restartPython()
```

## **Configuration**
Widgets are used to configure parameters:

- **SMB Credentials**:
  - `SMBClientUsername`: SMB username.
  - `SMBClientPassword`: SMB password stored in Azure Key Vault.

- **Azure Storage Details**:
  - `keyVaultName`: Azure Key Vault name for secrets.
  - `tgtStorageAccount`: Azure Blob Storage account name.
  - `tgtStorageAccountKeyName`: Key for accessing the storage account.
  - `tgtContainer`: Azure Blob Storage container name.

- **Source and Target Paths**:
  - `sourceFolderRootPath`: SMB server folder path.
  - `tgtFolderRootPath`: Azure Blob Storage folder path.

### **File Operations**
1. **List Files**: 
   Uses `smbclient.listdir` to list files in the SMB directory.

2. **File Transfer**:
   - **Calculate MD5 Hash of the Source File**:
     Reads the file in chunks for memory efficiency.
   - **Upload File to Azure Blob Storage**:
     Uploads the file using `upload_blob_to_container`.
   - **Verify MD5 Hash**:
     Downloads the uploaded file and compares its MD5 hash with the source file.

---

## **Code Flow**

### 1. **Initialize Widgets**:
```python
dbutils.widgets.text('SMBClientUsername', 'usiphdireporting@bp.com')
dbutils.widgets.text('SMBClientPassword', 'USIPHDIREPORTING-PWD-KV')
dbutils.widgets.text('keyVaultName', 'ZNEUDL1P48DEF-kvl01')
dbutils.widgets.text('sourceFolderRootPath', '<SMB folder path>')
dbutils.widgets.text('tgtStorageAccount', '<Azure storage account>')
dbutils.widgets.text('tgtStorageAccountKeyName', '<Access key name>')
dbutils.widgets.text('tgtContainer', '<Azure container>')
dbutils.widgets.text('tgtFolderRootPath', '<Azure folder path>')
```

### 2. Authenticate and List Files:
```python
smbclient.listdir(smb_file_path, username=SMB_CLIENT_NAME, password=SMB_CLIENT_PWD)
```

### 3. File Upload and Verification:
```python
with smbclient.open_file(smb_file_path, mode='rb', username=SMB_CLIENT_NAME, password=SMB_CLIENT_PWD) as source_file:
    source_md5 = calculate_md5(source_file)
    uploadSuccess = Utility.upload_blob_to_container(CONTAINER_CLIENT, azure_blob_name, source_file.read())
    if uploadSuccess:
        downloaded_blob = CONTAINER_CLIENT.get_blob_client(blob=azure_blob_name).download_blob().readall()
        downloaded_md5 = hashlib.md5(downloaded_blob).hexdigest()
        if source_md5 == downloaded_md5:
            print("✅ MD5 verification successful.")
        else:
            print("❌ MD5 verification failed.")

```

---

## **Key Components**
1. **SMB File Operations**:
   - Uses `smbclient` to list and read files from the SMB server.
   
2. **Azure Blob Storage Operations**:
   - Interacts with Azure Blob Storage using `azure-storage-blob`.

3. **File Integrity Check**:
   - Verifies file consistency using MD5 hashing.

---

## **How to Run**
1. Install the required libraries.
2. Configure widget values for SMB and Azure credentials and paths.
3. Execute the script in Databricks or a Python environment supporting `dbutils`.

```python
%run "SMB_to_Azure_Blob_Transfer"
```
