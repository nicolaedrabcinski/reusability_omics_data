import ftplib
import os
import tarfile

# Function to display download progress
def progress_callback(block):
    progress.update(len(block))
    file.write(block)


# Путь к папке, куда вы хотите загрузить и разархивировать файл
output_folder = "/home/nicolaedrabcinski/research/lab/reusability_omics_data/data/input/sra_metadata"

# Подключение к FTP серверу
with ftplib.FTP("ftp-trace.ncbi.nlm.nih.gov") as ftp:
    ftp.login()

    # Переходим в директорию с метаданными SRA
    ftp.cwd("/sra/reports/Metadata/")
    
    # Получаем список файлов на FTP сервере
    file_list = ftp.nlst()


tar_gz_files = [f for f in file_list if f.endswith('.tar.gz')]
file_to_download = tar_gz_files[0]

# Подключение к FTP серверу
while False:
    with ftplib.FTP("ftp-trace.ncbi.nlm.nih.gov") as ftp:
        ftp.login()

        # Переходим в директорию с метаданными SRA
        ftp.cwd("/sra/reports/Metadata/")
        
        # Скачиваем файл с FTP сервера с отображением прогресса
        with open(local_file_path, 'wb') as file, tqdm(unit='block', unit_scale=True, desc=file_to_download, leave=False) as progress:
            ftp.retrbinary(f"RETR {file_to_download}", callback=progress_callback)

local_file_path = os.path.join(output_folder, file_to_download)

extract_folder_name = os.path.splitext(os.path.splitext(file_to_download)[0])[0]
extract_folder_path = os.path.join(output_folder, extract_folder_name)

# Create the folder if it doesn't exist
if not os.path.exists(extract_folder_path):
    os.makedirs(extract_folder_path)

# Unarchive the downloaded file
with tarfile.open(local_file_path, "r:gz") as tar:
    # Extract the contents of the archive into the specified folder
    tar.extractall(extract_folder_path)

