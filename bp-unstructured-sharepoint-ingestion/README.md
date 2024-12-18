[[_TOC_]]

# Introduction
The purpose of this pattern is to copy data from a SharePoint site, either recursively or non-recursively. For recursive transfers, subfolders and their contents are copied to the destination while preserving the original folder structure. In non-recursive transfers, only the files from the specified SharePoint source folder are copied, excluding any subfolders. This data transfer pattern can handle both structured and unstructured files, ensuring that the data format is maintained from source to destination throughout the transfer process.

<br>

# Pattern Information

| Data Source                                                                                         | Data Type  | Data Target                                                         | Ingestion Type     | File Format                       |
|-----------------------------------------------------------------------------------------------------|------------|---------------------------------------------------------------------|--------------------|-----------------------------------|
| SharePoint Online site| Unstructured | ADLS Gen2 or Azure Blob Storage (ABS) | SNP or INC | Same as source (e.g. PDF, MP4, JPEG, HTML, pptx) |

<br>

# SPO File Ingestion Workflow
![unstruct_spo_file_ingestion.drawio.png](/.attachments/unstruct_spo_file_ingestion.drawio-f2ace949-1787-4f3a-ab1c-d96535e832fb.png)

- Azure Data Factory (ADF) orchestrates and manages the entire job execution.
- The process authenticates to the SharePoint Online (SPO) site using a service principal (SPN).
- The SPO paths to be copied are retrieved from the `unstruct_metadata.unstructpathmappings` control table, alongside other copying settings (e.g. `isRecursive`, `FilesToExclude`).
- Data from the SPO paths are copied to the target location.
- The `LastIngestionDate` column in the `unstruct_metadata.unstructpathmappings` table is updated to the timestamp of when the last ingestion was run.
- Successes and failures of folder copies are appended to the run log table (`unstruct_metadata.runlog_unified`); data that is skipped (either due to exclusion or during an INC load whereby the source file has not been modified) do not have rows appended to the run log table.

<br>

# Control Tables
The below tables refer to the **`[audit].[tblJobQueue]`** control table in P12 SQL Server, and the **`unstruct_metadata.unstructpathmappings`** table in P40 Databricks (mounted on ADLS):

<details><summary><span style="color:#009B00"><small>[audit].[tblJobQueue]</summary>

| <center>Column Name</center> | <center>Purpose</center> | <center>Example</center> |
|---------------------|---------------------------------|-------------------------------|
| <center>jobGroup</center> | Number representing logical grouping of job | 524 |
| <center>jobOrder</center> | after jobGroup, next level of logical separation of same request/use case if any special cases |    3   |
| <center>jobNum</center> | Number of jobs within JobGroup and JobOrder | 4 |
| <center> fkLoadType </center> | Load Strategy - <br>1. SNP -> for a full snapshot load<br>2. INC -> for incremental load (source files modified after the last data transfer are copied only) | SNP |
| <center> filterQuery </center> | SQL query retrieves the SPO source folder paths (to be copied) from the unstruct_metadata.unstructpathmappings control table | select * from unstruct_metadata.unstructpathmappings where jobQueueReference=524 and isActive='Y' and isRecursive='Y' and docHubPath like 'https://bp365.sharepoint.com/sites/GDN_WellsSubsurface/%' |
| <center> targetStorageAccount </center> | Target Azure Storage Account name where the data is to be copied to | zneusu5p82cggdata |
| <center> targetContainer </center> | Target Azure Storage Countainer name where the data is to be copied to | bptocgg |
| <center> targetFilePath </center> | Target folder path (relative to container) where the data is to be copied to | /Fluids_Data/ |
| <center> successNotificationEmailIDs </center> | Email that will receive the success notification | gdespowerdevteam@bp.com |
| <center> failureNotificationEmailIDs </center> | Email that will receive the success notification | gdespowerdevteam@bp.com |
| <center> isActive </center> | Switch to turn job on (Y) and off (N) | Y |
| <center> clusterPermissionGroupName </center> | AD Group that has the permissions to spawn a cluster in P40 Databricks | G DES POWER DEV TEAM |
| <center> clusterPermissionLevel </center> | Cluster permissions level to permit control of spawned clusters in P40 Databricks | CAN_MANAGE |
| <center> jobPath </center> | Path of the Databricks notebook (containing the code) to be invoked by ADF | /GDESPOWER/POWER_REPO/Notebooks/IngestionPatterns/Python/Unstructured - Sharepoint Files/Modular code/Unstructured_SP_File_Ingestion |
| <center> keyVaultName </center> | Key vault that stores the target connection id and password secrets | ZNEUDL1P40INGing-kvl00 |
| <center> sourceURL </center> | URL of the source SPO site that is to be authenticated to | https://bp365.sharepoint.com/sites/GDN_WellsSubsurface |
| <center> intermediateStoreConnID </center> | SPN client id for authenticating to the SPO site,  stored in key vault | PWR-SHAREPOINT-ONLINE-SPID-KV |
| <center> intermediateStoreSecretName </center> | SPN client secret for authenticating to the SPO site,  stored in key vault | PWR-SHAREPOINT-ONLINE-SPPWD-KV |
| <center> targetStoreConnID </center> | URL of the source SPO site that is to be authenticated to | accessKey-CGG-zneusu5p82cggdata |

</details>

<details><summary><span style="color:#009B00"><small>unstruct_metadata.unstructpathmappings</summary>

| <center>Column Name</center> | <center>Purpose</center> | <center>Example</center> |
|---------------------|---------------------------------|-------------------------------|
| <center>recordId</center> | Unique id for an SPO folder path | 1024 |
| <center>docHubPath</center> | SPO folder path being copied. Must end with a forward slash "/" | https://bp365.sharepoint.com/sites/GDN_NorthSea/Clair_Data/DATA/02 Compiled Data/ |
| <center>LastIngestionDate</center> | Timestamp of latest ingestion run | 2024-06-01 21:00:01.169557 |
| <center>jobQueueReference</center> | Equivalent to the jobGroup in the corresponding [audit].[tlblJobQueue] control table | 524 |
| <center>targetFilePath</center> | Uniform Resource Identifier specifying a folder location within the target ABS/ADLS | wasbs://bptocgg@zneusu5p82cggdata.blob.core.windows.net/Clair_Data/Clair_Data/DATA/02 Compiled Data/ |
| <center>FilesToExclude</center> | A comma-separated string containing words or phrases. A file will be excluded from copying if its name (excluding the extension) contains any of the words or phrases listed here. | Area Resource Progression Plan,Authorisation for Expenditure,Budget,Business Plan <br><br><br> select path from unstruct_metadata.atlantis_smartsearch where path like 'https://bp365.sharepoint.com/sites/%'  |
| <center>isActive</center> | Switch to turn copying for the specified SPO source folder on (Y) and off (N) | Y |
| <center>isRecursive</center> | Switch to turn recursive copying on (Y) or off (N) | Y |
| <center>FilesToInclude</center> | A dictionary used to specify which files you want to copy explicitly. It allows you to filter files based on their names (excluding extension) and extensions. The dictionary has 2 keys: <br><br>**1) fileName:** This key holds a comma-separated string containing words or phrases. A file will be included for copying if its name (excluding the extension) contains any of the words or phrases listed here. <br><br>**2) fileExt:** This key holds a comma-separated string containing file extensions. A file will be included for copying if its extension matches any of the extensions listed here. | {"fileName" : "hello", "fileExt" : "pdf,txt,mp4"} <br><br>*In this example, any file with the word "hello" (excluding the extension) in its name will be copied (e.g., "hello_world.txt", "myreport_hello.pdf"). Any file with an extension of ".pdf", ".txt", or ".mp4" will be copied (e.g., "document.pdf", "data.txt", "video.mp4").* |

</details>

<br>

# Prerequisites & Python Libraries
1 SPN with READ access to the SPO site. This can be done by raising a SNOW ticket.
2. Access keys in a key vault for accessing the target storage.
3. The following python libraries are necessary for the code:
- msal==1.22.0
  - _To connect to P12 SQL server `[audit].[tblJobQueue]` table._
- Office365-REST-Python-Client
  - *SharePoint API*
- azure-storage-blob==12.11.0
  - _To copy data from source to target._

<br>

# Pipeline Details
| <center>ADF</center> | <center>Pipeline</center> |
|--|--|
| <small>zneudl1pf8adf-prod-PWR-01</small> | <small>Unstructured File Ingestion - SPO/PL_PWR_UNSTRUCT_SPO_FILE_INGESTIONl</small> |

<br>

# Contacts
For queries or suggested improvements, please contact gdespowerdevteam@bp.com
<br>

# Abbreviations
This section defines the abbreviations that are used througout this Wiki page.
<details><summary><span style="color:#009B00"><small>Click to expand</summary>
Throughout this Wiki page, abbreviations are used as defined in the table below.

| Abbreviation | Definition |
|--|--|
| ADF | Azure Data Factory |
| ADLS | Azure Data Lake Storage |
| DBX | Databricks |
| SPN | Service Principal |
| SPO | SharePoint Online |
