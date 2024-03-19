import os
import datetime
import urllib.request
import tarfile

def download_publications():
    # Создаем директорию для скачанных публикаций
    date_today = datetime.datetime.now().strftime("%m-%d-%Y")
    publications_dir = f"/home/nicolaedrabcinski/research/lab/mangul-data-reusability/scripts/downloaded/publications_{date_today}"
    os.makedirs(publications_dir, exist_ok=True)

    # Переходим в директорию для скачанных публикаций
    os.chdir(publications_dir)

    # Создаем поддиректории для коммерческих и не-коммерческих публикаций
    commercial_dir = os.path.join(publications_dir, "commercial")
    noncommercial_dir = os.path.join(publications_dir, "noncommercial")
    os.makedirs(commercial_dir, exist_ok=True)
    os.makedirs(noncommercial_dir, exist_ok=True)

    # Скачиваем файлы для коммерческих и не-коммерческих публикаций
    ftp_base_url = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/"
    commercial_url = f"{ftp_base_url}oa_comm/xml/"
    noncommercial_url = f"{ftp_base_url}oa_noncomm/xml/"
    download_files(commercial_url, commercial_dir)
    download_files(noncommercial_url, noncommercial_dir)

def download_files(url, output_dir):
    # Получаем список файлов на FTP-сервере
    file_list = urllib.request.urlopen(url).read().decode("utf-8").splitlines()

    # Скачиваем каждый файл
    for file_name in file_list:
        file_url = f"{url}{file_name}"
        output_file = os.path.join(output_dir, file_name)
        urllib.request.urlretrieve(file_url, output_file)

def process_directory(input_dir):
    # Получаем список всех папок внутри директории input без субдиректорий
    directories = [os.path.join(input_dir, d) for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))]
    total_directories = len(directories)
    processed_directories = 0

    # Функция для обработки каждой папки
    for subdir in directories:
        processed_directories += 1
        print(f"Processing directory: {subdir}")
        print(f"Directories processed: {processed_directories} out of {total_directories}")

        # Создаем уникальные имена для выходных файлов, используя название текущей папки
        tmp_rawPubData = os.path.join("../data_lists/raw_pub_data", os.path.basename(subdir) + "_rawPubData.txt")
        tmp_journalNames = os.path.join("../data_lists/journal_names", os.path.basename(subdir) + "_journalNames.txt")
        outfile = os.path.join("../data_tables/pre_filter_matrices", os.path.basename(subdir) + "_preFilterMatrix.csv")

        # Извлекаем данные, содержащие SRA или GEO номера доступа из текущей папки
        grep_command = f"grep -o -r -E -H " \
                       f"-e '[SDE]R[APXRSZ][0-9]{{6,7}}' " \
                       f"-e 'PRJNA[0-9]{{6,7}}' " \
                       f"-e 'PRJD[0-9]{{6,7}}' " \
                       f"-e 'PRJEB[0-9]{{6,7}}' " \
                       f"-e 'GDS[0-9]{{1,6}}' " \
                       f"-e 'GSE[0-9]{{1,6}}' " \
                       f"-e 'GPL[0-9]{{1,6}}' " \
                       f"-e 'GSM[0-9]{{1,6}}' " \
                       f"{subdir}"
        os.system(f"{grep_command} > {tmp_rawPubData}")

        # Извлекаем названия журналов из текущей папки
        with open(tmp_rawPubData, 'r') as file:
            journal_data = file.read()
        journal_ids = []
        for match in re.finditer(r'<journal-meta>.*?<journal-id journal-id-type="nlm-ta">(.*?)</journal-id>', journal_data):
            journal_ids.append(match.group(1))
        with open(tmp_journalNames, 'w') as file:
            file.write("journal\n")
            file.write("\n".join(journal_ids))

        # Форматируем сырые данные в CSV
        with open(tmp_rawPubData, 'r') as file:
            raw_data = file.readlines()
        pmc_accs_data = []
        for line in raw_data:
            fields = line.strip().split("/")
            pmc_accs_data.append(f"{fields[13]},{fields[15]}")
        with open(os.path.join("../data_lists/pmc_accs", os.path.basename(subdir) + "_pmcAndAccs.csv"), 'w') as file:
            file.write("pmc_ID,accession\n")
            file.write("\n".join(pmc_accs_data))

        # Объединяем названия журналов и доступы в один CSV файл
        with open(outfile, 'w') as file:
            with open(tmp_journalNames, 'r') as journal_file:
                journal_names = journal_file.readlines()[1:]
            with open(os.path.join("../data_lists/pmc_accs", os.path.basename(subdir) + "_pmcAndAccs.csv"), 'r') as pmc_file:
                pmc_accs = pmc_file.readlines()[1:]
            for journal, pmc_acc in zip(journal_names, pmc_accs):
                file.write(journal.strip() + "," + pmc_acc)

def main():
    # Загрузка публикаций
    download_publications()

    # Обработка папок
    input_dir = "/home/nicolaedrabcinski/research/lab/mangul-data-reusability/scripts/downloaded_files/pmcOA_03-12-2024/"
    process_directory(input_dir)

    print("Processing completed.")

if __name__ == "__main__":
    main()
