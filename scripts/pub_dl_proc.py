import ftplib
import tarfile
import os
import re
import shutil
import logging

from tqdm import tqdm

# Настройка уровня логирования
logging.basicConfig(
    filename='pub_dl_proc.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

custom_bar_format = "{l_bar}{bar} {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
journal_regex_pattern = r'<journal-meta>.*?<journal-id journal-id-type="nlm-ta">(.*?)</journal-id>'


# FTP connection settings

ftp_servers = [
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_comm/xml/'),
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_noncomm/xml/'),
    ('ftp.ncbi.nlm.nih.gov', '/pub/pmc/oa_bulk/oa_other/xml/')
]

# ftp_server = 'ftp.ncbi.nlm.nih.gov'
# ftp_dir = '/pub/pmc/oa_bulk/oa_comm/xml/'
local_dir = '../data/input/publications'

def extract_tar_gz(archive_path, extract_dir):
    with tarfile.open(archive_path, 'r:gz') as tar:
        tar.extractall(extract_dir)

def search_journal_xml(directory_path, regex_pattern):
    # Список для хранения совпадений
    matches = []

    # Перебираем все файлы в директории
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            # Проверяем, что файл имеет расширение XML
            if file_name.endswith('.xml'):
                # Формируем путь к файлу
                file_path = os.path.join(root, file_name)
                
                # Открываем файл и читаем его содержимое
                with open(file_path, 'r') as file:
                    content = file.read()

                    # Ищем все совпадения по регулярному выражению
                    matches.extend(re.findall(regex_pattern, content))

    return matches

def process_file(file_path):
    print("File path: ", file_path)
    # Создаем уникальные имена для выходных файлов, используя название текущего файла
    tmp_raw_pub_data = os.path.join("../data/interim/raw_pub_data", os.path.basename(file_path) + "_raw_pub_data.txt")
    tmp_journal_names = os.path.join("../data/interim/journal_names", os.path.basename(file_path) + "_journal_names.txt")
    outfile = os.path.join("../data/processed/pre_filter_matrices", os.path.basename(file_path) + "_pre_filter_matrix.csv")

    # Извлекаем данные, содержащие SRA или GEO номера доступа из текущего файла
    grep_command = f"grep -o -r -E -H " \
                   f"-e '[SDE]R[APXRSZ][0-9]{{6,7}}' " \
                   f"-e 'PRJNA[0-9]{{6,7}}' " \
                   f"-e 'PRJD[0-9]{{6,7}}' " \
                   f"-e 'PRJEB[0-9]{{6,7}}' " \
                   f"-e 'GDS[0-9]{{1,6}}' " \
                   f"-e 'GSE[0-9]{{1,6}}' " \
                   f"-e 'GPL[0-9]{{1,6}}' " \
                   f"-e 'GSM[0-9]{{1,6}}' " \
                   f"{file_path}"
    os.system(f"{grep_command} > {tmp_raw_pub_data}")

    journal_names = search_journal_xml(file_path, journal_regex_pattern)
    print("Journal names: ", journal_names)
    
    with open(tmp_journal_names, 'w') as file:
        file.write("journal\n")
        file.write("\n".join(journal_names))

    # Форматируем сырые данные в CSV
    with open(tmp_raw_pub_data, 'r') as file:
        raw_data = file.readlines()
        print(raw_data)
    
    pmc_accs_data = [line.strip().split("/")[-1].replace('.xml', '').rpartition(":")[0] + "," + line.strip().split("/")[-1].replace('.xml', '').rpartition(":")[2] for line in raw_data]
    print('Accessions: ', pmc_accs_data)
    
    with open(os.path.join("../data/interim/accessions", os.path.basename(file_path) + "_accessions.csv"), 'w') as file:
        file.write("pmc_id,accession\n")
        file.write("\n".join(pmc_accs_data))

    # Объединяем названия журналов и доступы в один CSV файл
    with open(outfile, 'w') as file:
        file.write('journal_name,pmc_id,accession\n')
        with open(tmp_journal_names, 'r') as journal_file:
            journal_names = journal_file.readlines()[1:]
        with open(os.path.join("../data/interim/accessions", os.path.basename(file_path) + "_accessions.csv"), 'r') as pmc_file:
            pmc_accs = pmc_file.readlines()[1:]
        for journal, pmc_acc in zip(journal_names, pmc_accs):
            file.write(journal.strip() + "," + pmc_acc)

def conn_ftp(file_list, ftp_server, ftp_dir):
    # Loop through each file in the file list
    for i, filename in enumerate(tqdm(file_list, desc='Processing ...')):
        archive_path = os.path.join(local_dir, filename)
        extract_dir = os.path.splitext(os.path.splitext(archive_path)[0])[0]
        os.makedirs(extract_dir, exist_ok=True)

        # Connect to FTP server for downloading the file
        with ftplib.FTP(ftp_server, timeout=7200) as ftp_file:
            ftp_file.login()
            ftp_file.cwd(ftp_dir)

            # Download the file
            with open(archive_path, 'wb') as file:
                ftp_file.retrbinary(f'RETR {filename}', file.write)

        # Extract the downloaded file
        extract_tar_gz(archive_path, extract_dir)

        # Process the extracted files
        process_file(extract_dir)

        # Optionally, remove the downloaded archive after processing
        shutil.rmtree(extract_dir)
        os.remove(archive_path)

        # Update the progress bar
        tqdm.write(f'{i + 1} files processed')

com_ftp_server, com_ftp_dir = ftp_servers[0]  
non_com_ftp_server, non_com_ftp_dir = ftp_servers[1]
other_ftp_server, other_ftp_dir = ftp_servers[2]

with ftplib.FTP(com_ftp_server, timeout=7200) as ftp:
    ftp.login()
    ftp.cwd(com_ftp_dir)
    file_list = ftp.nlst()
    com_tar_gz_files = [file for file in file_list if file.endswith('.tar.gz')]

with ftplib.FTP(non_com_ftp_server, timeout=7200) as ftp:
    ftp.login()
    ftp.cwd(non_com_ftp_dir)
    file_list = ftp.nlst()
    non_com_tar_gz_files = [file for file in file_list if file.endswith('.tar.gz')]

with ftplib.FTP(other_ftp_server, timeout=7200) as ftp:
    ftp.login()
    ftp.cwd(other_ftp_dir)
    file_list = ftp.nlst()
    other_tar_gz_files = [file for file in file_list if file.endswith('.tar.gz')]

os.makedirs(local_dir, exist_ok=True)

conn_ftp(com_tar_gz_files, com_ftp_server, com_ftp_dir)
conn_ftp(non_com_tar_gz_files, non_com_ftp_server, non_com_ftp_dir)
conn_ftp(other_tar_gz_files, other_ftp_server, other_ftp_dir)



    