[[_TOC_]]

# Introduction
The purpose of this pattern is to copy data from a NetApp File Share source via Server Message Block (SMB), either recursively or non-recursively. For recursive transfers, subfolders and their contents are copied to the destination while preserving the original folder structure. In non-recursive transfers, only the files from the specified NetApp source folder are copied, excluding any subfolders. This data transfer pattern can handle both structured and unstructured files, ensuring that the data format is maintained from source to destination throughout the transfer process.

## SMB
We connect to the NetApp source via SMB. SMB is a network protocol primarily used for sharing access to files, printers, and serial ports between nodes on a network. SMB was originally developed by IBM in the 1980s. Microsoft then adopted and extended the protocol, which became a core part of Windows networking features. It can also be used with other operating systems, such as Linux and macOS, through compatible software like Samba, which implements the SMB protocol for Unix systems.

SMB allows applications and users to read and write to files and request services from server programs in a computer network. It is used extensively in Microsoft Windows for network file browsing and sharing. It includes mechanisms for user authentication and authorization.
 

<br>

----------------------------------------------------------------------------------------------------

# Pattern Information

| Data Source                                                                                         | Data Type  | Data Target                                                         | Ingestion Type     | File Format                       |
|-----------------------------------------------------------------------------------------------------|------------|---------------------------------------------------------------------|--------------------|-----------------------------------|
| NetApp File Share | Unstructured | ADLS Gen2 or Azure Blob Storage (ABS) | SNP or INC | Same as source (e.g. PDF, MP4, JPEG, HTML, pptx) |

<br>

----------------------------------------------------------------------------------------------------

# NetApp File Ingestion Workflow

![image](https://github.com/user-attachments/assets/485b7221-6bb7-440e-bf45-e6f4b81f55f3)


- The process comprises of 3 steps:
   1. Listing files from the NetApp source into the `unstruct_metadata.netapp_configuration` table.
   1. Classifying the listed files in the table.
   1. Copying the non-restricted files in the table.

## Step 1
   - Connects to the NetApp source via SMB.
   - (Non-)Recursively lists files at the specified NetApp source folder into the table `unstruct_metadata.netapp_configuration`.

## Step 2 (classification)
   - Retrieves exclusion criteria from 4 existing DBX tables:
     - `unstruct_metadata.exclusion_abbreviation`
     - `unstruct_metadata.exclusion_extension`
     - `unstruct_metadata.exclusion_filepath`
     - `unstruct_metadata.exclusion_folderpath`
   - Updates the `unstruct_metadata.netapp_configuration` table, classifying files as to whether they should be copied or not. This is indicated by the `toBeExcluded` column which if is set to `Y` then the file should not be copied in Step 3, vice versa.  

## Step 3 (copying to target Blob)
   - Copies files listed in `unstruct_metadata.netapp_configuration` table where `toBeExcluded` is `N`.
   - Copying is done is parallel, where the files for the specified jobGroup and jobOrder in `unstruct_metadata.netapp_configuration` table are split into groups of 200 and are processed in 20 parallel DBX notebooks at one time.
   - Updates the `unstruct_metadata.netapp_configuration` table, specifying if the copy was successful, the copying timestamp, and the target location that the file was copied.
   - If the copy failed, the exception is captured.


<br>

----------------------------------------------------------------------------------------------------


# Control Tables
Up to 7 tables are involved in this pattern, shown in the table below along with their purpose:

| Table | Purpose |
|--|--|
| `[audit].[tblJobQueue]` | Stores control parameters that are inputs to the pattern, enabling the management and scheduling of jobs. |
| `unstruct_metadata.netapp_configuration` | 	Contains metadata for listing, classification, and copying processes, which is updated throughout steps 1, 2, and 3 of the pattern. |
| `unstruct_metadata.exclusion_abbreviation` | Lists abbreviations, words, or strings that, if found in a file name, will result in the file being excluded from the copying step (`toBeExcluded` is set to Y). |
| `unstruct_metadata.exclusion_extension` | Lists file extensions that, if found, will result in the file being excluded from the copying step (`toBeExcluded` is set to Y). |
| `unstruct_metadata.exlusion_filepath` | Contains full NetApp file paths that, if matched, will result in the file being excluded from the copying step (`toBeExcluded` is set to Y). |
| `unstruct_metadata.exlusion_folderpath` | Contains full NetApp folder paths that, if matched, will result in all files within the folder being excluded from the copying step (`toBeExcluded` is set to Y). |
| `unstruct_metadata.runlog_unified` | Stores metadata for run logs, capturing both successes and failures during the copying step (step 3). |
| `unstruct_metadata.listing_failures_{jobGroup}_{jobOrder}` | Stores failures that occur during the listing process in step 1. These failures can include user access denied errors to a NetApp source folder/file. Such issues must be resolved before continuing with the pattern. To bypass these failures quickly, users can add the inaccessible folder or file paths to the appropriate exclusion tables listed above. |


## `[audit].[tblJobQueue]`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Variable Name | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job | 600 |
| jobOrder | Number representing logical sub-grouping within the jobGroup | 2 |
| SMBClientUsername | Username required for authenticating with the SMB server to access and interact with files | usiphdireporting@bp.com |
| SMBClientPassword | Password required for authenticating with the SMB server to access and interact with files | USIPHDIREPORTING-PWD-KV |
| keyVaultName | Key vault that stores the SMB password secret | ZNEUDL1P48DEF-kvl01 |
| sourceURL | NetApp source folder to be copied (non-)recursively | aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/Great_White/Fieldwide/Presentations_Meetings/Project/Asset_Mgmt/Meetings/2008_JV_HW+PRH_Mtgs |
| failuresTableName | Table name to store failures that may occur during the step 1 listing process _(e.g. if certain folders cannot be accessed due to restricted permissions)_ | unstructured_failedlog_recursive_nl |
| fkTargetFileFormat | File format of the target table | delta |
| comments | Contain the dictionary with the boolean value of whether the ingestion is recursive or not | {"recursive":True} |
| fkLoadType | File ingestion load type for files with `toBeExcluded='N'`: <br><br> **SNP:** All listed files are to be copied. <br><br> **INC:** Only files with a `modifiedTimeStamp` later than the `copyTimeStamp` or a `null` value in the `status`, `copyStatusTimeStamp`, and `modifiedTimeStamp` are copied. | SNP |

</details>


## `unstruct_ metadata.netapp_configuration`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Column | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job. | 600 |
| jobOrder | Specifies the logical sub-grouping within the `jobGroup`. | 2 |
| sourceFilePath  | Path to the source file on the NetApp network. | aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/Great_White/Fieldwide/File.pdf |
| isAtSource | Indicates whether the file is present at the NetApp source (`Y` for yes, `N` for no). | Y |
| fileSizeMB | Size of the file in megabytes, accurate to three decimal places. | 3.455 |
| createdTimeStamp | Timestamp when the file was created. | 2012-06-12T19:22:55.619+00:00 |
| modifiedTimeStamp | Timestamp when the file was last modified. | 2012-05-24T21:04:18.792+00:00 |
| listingTimeStamp | Timestamp when the file was added to the `unstruct_metadata.netapp_configuration` table. | 2024-08-06T10:20:34.018+00:00 |
| toBeExcluded | Indicates whether the file should be excluded from the copying process in step 3 (`Y` for yes, `N` for no). | N |
| exclusionReason | Reason for excluding the file, if applicable; otherwise, it is `null`. | `null` |
| exclusionTimeStamp | Timestamp when the classification (step 2) was applied to the file. | 2024-08-06T10:28:55.564+00:00 |
| targetFilePath | Destination path where the file was copied during step 3 (copying process). If `toBeExcluded` is `Y`, this remains as `null`. | https://zneudl1p40ingstor01.blob.core.windows.net/unstructured-data/Great_White_Data/NetApp/OBO_Great_White/Great_White/Fieldwide/File.pdf |
| status | Indicates whether the file was successfully copied to the targetFilePath (`S` for succeeded, `F` for failed). | S |
| copyFailReason | Reason for any copy failures; otherwise, it remains `null`. |  |
| copyStatusTimeStamp | Timestamp when step 3 (copying process) was executed for the file. | 2024-08-06T10:37:50.120+00:00 |

</details>


## `unstruct_ metadata.exclusion_abbreviation`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Column | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job. | 600 |
| jobOrder | Specifies the logical sub-grouping within the `jobGroup`. | 2 |
| abbreviation | Comma-separated 
abbreviations/strings that, if found in a file name, will result in the file being excluded from the copying step (`toBeExcluded` is set to Y). | TAM,Business Account,FFM |

</details>


## `unstruct_ metadata.exclusion_extension`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Column | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job. | 600 |
| jobOrder | Specifies the logical sub-grouping within the `jobGroup`. | 2 |
| extension | Comma-separated file extensions that, if found, will result in the file being excluded from the copying step (`toBeExcluded` is set to Y). | aprx,txt,pps |

</details>


## `unstruct_ metadata.exlusion_filepath`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Column | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job. | 600 |
| jobOrder | Specifies the logical sub-grouping within the `jobGroup`. | 2 |
| filePath | A full NetApp file path that, if matched, will result in the file being excluded from the copying step (`toBeExcluded` is set to Y). | aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/Great_White/Fieldwide/this_file_will_be_excluded.pdf |

</details>


## `unstruct_ metadata.exlusion_folderpath`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Column | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job. | 600 |
| jobOrder | Specifies the logical sub-grouping within the `jobGroup`. | 2 |
| folderPath | A full NetApp folder path that, if matched, will result in all files within the folder being excluded from the copying step (`toBeExcluded` is set to Y). | aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/Great_White/this_folder_will_be_excluded |

</details>


## `unstruct_ metadata.runlog_unified`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>

| Column | Purpose | Example |
|--|--|--|
| jobGroup | Number representing logical grouping of job. | 600 |
| jobOrder | Specifies the logical sub-grouping within the `jobGroup`. | 2 |
| starTime | Timestamp when the copying process (step 3) begun. | 2024-08-06T10:33:50.270+00:00 |
| endTime | Timestamp when an attempt was made to copy the file (during step 3). | 2024-08-06T10:37:50.120+00:00 |
| status | Indicates whether the file was successfully copied to the targetFilePath (`S` for succeeded, `F` for failed). | S |
| sourceFilePath | Path to the source file on the NetApp network. | aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/OBO_Great_White/Great_White/Fieldwide/File.pdf |
| targetFilePath | Destination path where the file was expected to be copied to during step 3 (copying process). | https://zneudl1p40ingstor01.blob.core.windows.net/unstructured-data/Great_White_Data/NetApp/OBO_Great_White/Great_White/Fieldwide/File.pdf |
| fileName | The file name including the extension. | File.pdf |
| fileSizeMB | Size of the file in megabytes, accurate to three decimal places. | 3.455 |
| _year | The year taken from the starTime | 2024 |
| _month | The month taken from the starTime | 2 |
| errorMessage | Error message upon failure, otherwise remains as `null` |  |

</details>


## `unstruct_ metadata.listing_ failures_ {jobGroup}_ {jobOrder}`
<details><summary><span style="color:#009B00"><small>Click to expand</summary>
[job_group, JOB_ORDER, smb_folder_path, str(e), datetime.now()]

</details>

<br>

----------------------------------------------------------------------------------------------------



# Libraries
The libraries used throughout this pattern are listed and described in this section.


### os
A built-in Python library that provides a way of using operating system-dependent functionality, such as file manipulation, environment variables, and process management. It ensures that your code is portable across different operating systems by handling path separators appropriately (better than manual string manipulation). It makes your path construction cleaner and less error-prone compared to manually adding separators.

<details><summary><span style="color:#009B00"><small>Application in pattern</summary>

![image.png](/.attachments/image-d38d8b19-5230-4500-94e5-41121c35d926.png)
Here the `os` library is used to handle file and directory paths in a way that's compatible with the operation system that the code is running on. 

- `os.path.join()` combines 1 or more path components intelligently. It takes care of inserting the appropriate directory separator (/ on Unix-based systems like Linux and macOS, or \ on Windows) between the different parts of the path.

- `os.path.join(smb_folder_path, i)` creates a complete file path by joining `smb_folder_path` (which is the path to a directory) with `i` (which represents an individual file/sub-directory). This ensures that the resulting item_path is correctly formatted for the operating system.

 _Example:_
  | Type | Variable | Value |
  |--|--|--|
  | Input | `smb_folder_path` | `'/home/user/folder'` |
  | Input | `i` | `'file.txt'` |
  | Output | `os.path.join(smb_folder_path, i)` | `'/home/user/folder/file.txt'` on Unix-based systems <br><br> `'C:\\home\\user\\folder\\file.txt'` on Windows systems|

</details>




### re
A built-in Python library for working with regular expressions, which are patterns used to match and manipulate strings.


<details><summary><span style="color:#009B00"><small>Application in pattern</summary>

![image.png](/.attachments/image-38f2d975-27f2-4155-9c14-a7b340fd9609.png)
Here the `re` library is used for pattern matching.

- `re.compile` compiles a regular expression pattern into a regex object, which can then be used to perform pattern matching operations. It is useful when you need to use the same pattern multiple times, as it can be more efficient.
- `re.compile('|'.join(terms), re.IGNORECASE)` creates a regex pattern object where:
   -  `|` (the pipe character) is used to denote an "or" condition between the terms. So, `|.join(terms)` creates a regex pattern that matches any of the terms in the terms list or set.
   - `re.IGNORECASE` is a flag that makes the matching case-insensitive, so it will match terms regardless of whether they are in uppercase or lowercase.

- `.search()` method of the regex object scans through the given string and checks if any part of it matches the regex pattern.
- `re.compile('|'.join(terms), re.IGNORECASE).search(string)` searches the string for any occurrence of the pattern created from the terms. If a match is found, it returns a match object; otherwise, it returns `None`.

_Example:_
  | Type | Variable | Value |
  |--|--|--|
  | Input | `terms` | `{'cat', 'dog'}` |
  | Input | `string` | `I have a Dog` |
  | Output | `\|'.join(terms)` | `"cat\|dog"` |
  | Output | `re.compile("cat\|dog", re.IGNORECASE)` | `True` because it matches both "cat" and "dog" in the string, regardless of case |

</details>




### smbclient
A third-party library that allows Python code to interact with SMB/CIFS network file systems, typically used for accessing shared folders over a network.

<details><summary><span style="color:#009B00"><small>Application in pattern</summary>

This library has several uses in this pattern, used in step 1 to list directories/files at the NetApp source and to fetch metadata and in step 3 to copy data from the NetApp source to the Azure Blob destination. Here we exemplify connecting to the NetApp source via SMB protocol in order to list the contents of a directory.

![image.png](/.attachments/image-f2420d67-d9e6-479e-a296-c7df8afdfb87.png)

Here the `smbclient` library is used to list the contents of a directory on an SMB (Server Message Block) network share.

- `smbclient.listdir()` is a function provided by the `smbclient` library that retrieves a list of files and directories from a specified directory on an SMB network share using the provided `username` and `password`. It then accesses the directory specified by `smb_folder_path` and retrieves a list of items (files and subdirectories) within that directory. The function returns a list of item names, which you can then process or use as needed in your code.

_Example:_
  | Type | Variable | Value |
  |--|--|--|
  | Input | `smb_folder_path` | `\\server\share\folder` |
  | Input | `username` | _The correct username_ |
  | Input | `password` | _The correct password_ |
  | Output | `smbclient.listdir(smb_folder_path, username, password)` | Connects to the SMB server at `\\server\share\folder` using the credentials `smb_client_name` and `smb_client_pwd`, retrieves the list of files and directories in that path, and finally stores this list in the `items` variable. |

> Note: To use `smbclient`, you generally need to ensure it's properly configured and that your Python environment has access to the necessary SMB server. You may also need to handle exceptions or errors, such as network issues or authentication failures, which can occur during the directory listing process.

</details>




### threading
A built-in Python library that provides tools for creating and managing threads in a program, allowing for parallel execution of code. It provides a high-level interface for both thread-based and process-based parallelism.

<details><summary><span style="color:#009B00"><small>Application in pattern</summary>

![image.png](/.attachments/image-116923af-e73c-4da2-a289-398c2f22eca9.png)

Here the `threading` library is used to parallelise the execution of multiple tasks.

- `ThreadPoolExecutor` creates a thread pool with a maximum of 20 workers. This is what executes tasks concurrently.
- The code then iterates over a list of `df_chunk_queries` and submits each to the executor using the submit method. The `run_receiver_notebook` function is called for each query, with several arguments passed to it.
- The `submit` method returns a `Future` object (contained in `futures` list) for each submitted task. These objects represent the eventual result of the task.
- The `concurrent.futures.wait()` function is used to block the main thread until all submitted tasks have completed.

_Example:_

```
import concurrent.futures

def my_function(x):
    return x * x

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(my_function, num) for num in range(10)]

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

        # Process results (optional)
        for future in futures:
            print(future.result())
```
- We define a simple function my_function that squares a number.
- We create a thread pool with 4 workers.
- We submit 10 tasks to the executor, each calculating the square of a number from 0 to 9.
- We wait for all tasks to finish.
- We print the results of each task.

</details>



### datetime
A built-in Python library that provides classes for manipulating dates and times. You can get the current time, create specific dates, and format them as needed.


### pandas
A powerful third-party Python library for data manipulation and analysis. It provides data structures like DataFrames, which make it easy to work with structured data.

### json
A built-in Python library for working with JSON data (JavaScript Object Notation), which is commonly used for data exchange between a client and a server.

### concurrent.futures
A built-in Python library for running tasks concurrently, either through threading or multiprocessing. It provides a high-level interface for asynchronously executing callables.

