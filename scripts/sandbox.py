import ftplib
import os
import tarfile
import xml.etree.ElementTree as ET
import csv

def progress_callback(block):
    progress.update(len(block))
    file.write(block)

def sample_xml_to_csv(xml_file, csv_file):
    # Парсим XML файл
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Открываем CSV файл для записи
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Записываем заголовки столбцов в CSV файл
        headers = ['Sample Alias', 'Sample Accession', 'Taxon ID', 'Scientific Name', 'Bioproject ID', 'Strain', 'Isolation Source', 'Collection Date', 'Geo Location Name', 'Sample Type', 'BioSample Model']
        csvwriter.writerow(headers)

        # Итерируемся по каждому SAMPLE тегу
        for sample in root.findall('.//SAMPLE'):
            # Извлекаем данные образца
            alias = sample.attrib.get('alias', '')
            accession = sample.attrib.get('accession', '')
            taxon_id = sample.find('./SAMPLE_NAME/TAXON_ID').text if sample.find('./SAMPLE_NAME/TAXON_ID') is not None else ''
            scientific_name = sample.find('./SAMPLE_NAME/SCIENTIFIC_NAME').text if sample.find('./SAMPLE_NAME/SCIENTIFIC_NAME') is not None else ''
            bioproject_id = sample.find('./SAMPLE_LINKS/SAMPLE_LINK/XREF_LINK[ID="bioproject"]/LABEL').text if sample.find('./SAMPLE_LINKS/SAMPLE_LINK/XREF_LINK[ID="bioproject"]/LABEL') is not None else ''
            strain = sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="strain"]/VALUE').text if sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="strain"]/VALUE') is not None else ''
            isolation_source = sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="isolation_source"]/VALUE').text if sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="isolation_source"]/VALUE') is not None else ''
            collection_date = sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="collection_date"]/VALUE').text if sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="collection_date"]/VALUE') is not None else ''
            geo_loc_name = sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="geo_loc_name"]/VALUE').text if sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="geo_loc_name"]/VALUE') is not None else ''
            sample_type = sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="sample_type"]/VALUE').text if sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="sample_type"]/VALUE') is not None else ''
            biosample_model = sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="BioSampleModel"]/VALUE').text if sample.find('./SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG="BioSampleModel"]/VALUE') is not None else ''

            # Записываем данные образца в CSV файл
            csvwriter.writerow([alias, accession, taxon_id, scientific_name, bioproject_id, strain, isolation_source, collection_date, geo_loc_name, sample_type, biosample_model])

def experiment_xml_to_csv(xml_file, csv_file):
    # Парсим XML файл
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Открываем CSV файл для записи
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Записываем заголовки столбцов в CSV файл
        headers = ['Experiment Accession', 'Experiment Alias', 'Primary ID', 'Submitter ID', 'Title', 'Study Accession', 'Design Description', 'Sample Accession', 'Library Name', 'Library Strategy', 'Library Source', 'Library Selection', 'Instrument Model']
        csvwriter.writerow(headers)

        # Итерируемся по каждому EXPERIMENT тегу
        for experiment in root.findall('.//EXPERIMENT'):
            # Извлекаем данные эксперимента
            accession = experiment.attrib.get('accession', '')
            alias = experiment.attrib.get('alias', '')
            primary_id = experiment.find('./IDENTIFIERS/PRIMARY_ID').text if experiment.find('./IDENTIFIERS/PRIMARY_ID') is not None else ''
            submitter_id = experiment.find('./IDENTIFIERS/SUBMITTER_ID').text if experiment.find('./IDENTIFIERS/SUBMITTER_ID') is not None else ''
            title = experiment.find('./TITLE').text if experiment.find('./TITLE') is not None else ''
            study_accession = experiment.find('./STUDY_REF/IDENTIFIERS/PRIMARY_ID').text if experiment.find('./STUDY_REF/IDENTIFIERS/PRIMARY_ID') is not None else ''
            design_description = experiment.find('./DESIGN/DESIGN_DESCRIPTION').text if experiment.find('./DESIGN/DESIGN_DESCRIPTION') is not None else ''
            sample_accession = experiment.find('./DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/PRIMARY_ID').text if experiment.find('./DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/PRIMARY_ID') is not None else ''
            library_name = experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_NAME').text if experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_NAME') is not None else ''
            library_strategy = experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY').text if experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY') is not None else ''
            library_source = experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SOURCE').text if experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SOURCE') is not None else ''
            library_selection = experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SELECTION').text if experiment.find('./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SELECTION') is not None else ''
            instrument_model = experiment.find('./PLATFORM/ILLUMINA/INSTRUMENT_MODEL').text if experiment.find('./PLATFORM/ILLUMINA/INSTRUMENT_MODEL') is not None else ''

            # Записываем данные эксперимента в CSV файл
            csvwriter.writerow([accession, alias, primary_id, submitter_id, title, study_accession, design_description, sample_accession, library_name, library_strategy, library_source, library_selection, instrument_model])

def submission_xml_to_csv(xml_file, csv_file):
    # Парсим XML файл
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Открываем CSV файл для записи
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Записываем заголовки столбцов в CSV файл
        headers = ['Lab Name', 'Center Name', 'Accession', 'Alias']
        csvwriter.writerow(headers)

        # Итерируемся по каждому SUBMISSION тегу
        for submission in root.findall('.//SUBMISSION'):
            # Извлекаем данные submission
            lab_name = submission.attrib.get('lab_name', '')
            center_name = submission.attrib.get('center_name', '')
            accession = submission.attrib.get('accession', '')
            alias = submission.attrib.get('alias', '')

            # Записываем данные submission в CSV файл
            csvwriter.writerow([lab_name, center_name, accession, alias])

def run_xml_to_csv(xml_file, csv_file):
    # Парсим XML файл
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Открываем CSV файл для записи
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Записываем заголовки столбцов в CSV файл
        headers = ['Accession', 'Alias', 'Primary ID', 'Submitter ID', 'Experiment Accession', 'Experiment Submitter ID']
        csvwriter.writerow(headers)

        # Итерируемся по каждому RUN тегу
        for run in root.findall('.//RUN'):
            # Извлекаем данные run
            accession = run.attrib.get('accession', '')
            alias = run.attrib.get('alias', '')
            primary_id = run.find('./IDENTIFIERS/PRIMARY_ID').text if run.find('./IDENTIFIERS/PRIMARY_ID') is not None else ''
            submitter_id = run.find('./IDENTIFIERS/SUBMITTER_ID').text if run.find('./IDENTIFIERS/SUBMITTER_ID') is not None else ''
            experiment_ref = run.find('./EXPERIMENT_REF')
            experiment_accession = experiment_ref.attrib.get('accession', '') if experiment_ref is not None else ''
            experiment_submitter_id = experiment_ref.find('./IDENTIFIERS/SUBMITTER_ID').text if experiment_ref is not None and experiment_ref.find('./IDENTIFIERS/SUBMITTER_ID') is not None else ''

            # Записываем данные run в CSV файл
            csvwriter.writerow([accession, alias, primary_id, submitter_id, experiment_accession, experiment_submitter_id])

def study_xml_to_csv(xml_file, csv_file):
    # Парсим XML файл
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Открываем CSV файл для записи
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Записываем заголовки столбцов в CSV файл
        headers = ['Accession', 'Alias', 'Primary ID', 'External ID Namespace', 'External ID Label', 'Study Title', 'Study Type', 'Study Abstract']
        csvwriter.writerow(headers)

        # Итерируемся по каждому STUDY тегу
        for study in root.findall('.//STUDY'):
            # Извлекаем данные study
            accession = study.attrib.get('accession', '')
            alias = study.attrib.get('alias', '')
            primary_id = study.find('./IDENTIFIERS/PRIMARY_ID').text if study.find('./IDENTIFIERS/PRIMARY_ID') is not None else ''
            external_id = study.find('./IDENTIFIERS/EXTERNAL_ID')
            external_id_namespace = external_id.attrib.get('namespace', '') if external_id is not None else ''
            external_id_label = external_id.attrib.get('label', '') if external_id is not None else ''
            study_title = study.find('./DESCRIPTOR/STUDY_TITLE').text if study.find('./DESCRIPTOR/STUDY_TITLE') is not None else ''
            study_type = study.find('./DESCRIPTOR/STUDY_TYPE').attrib.get('existing_study_type', '') if study.find('./DESCRIPTOR/STUDY_TYPE') is not None else ''
            study_abstract = study.find('./DESCRIPTOR/STUDY_ABSTRACT').text if study.find('./DESCRIPTOR/STUDY_ABSTRACT') is not None else ''

            # Записываем данные study в CSV файл
            csvwriter.writerow([accession, alias, primary_id, external_id_namespace, external_id_label, study_title, study_type, study_abstract])


output_folder = "../data/input/sra_metadata"

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
print(extract_folder_path)

# Create the folder if it doesn't exist
if not os.path.exists(extract_folder_path):
    os.makedirs(extract_folder_path)

# Unarchive the downloaded file
# with tarfile.open(local_file_path, "r:gz") as tar:
#     # Extract the contents of the archive into the specified folder
#     tar.extractall(extract_folder_path)

csv_folder_path = os.path.join("../data/interim/sra_metadata", os.path.basename(extract_folder_name))
print(csv_folder_path)

# Создаем папку для сохранения CSV файлов, если она не существует
if not os.path.exists(csv_folder_path):
    os.makedirs(csv_folder_path)

# Рекурсивный обход директорий
for root_dir, dirs, files in os.walk(extract_folder_path):

    for file in files:

        xml_file_path = os.path.join(root_dir, file)
        print(xml_file_path)
        csv_file_path = os.path.join(csv_folder_path, os.path.splitext(file)[0] + ".csv")
        print(csv_file_path)

        if file.endswith("sample.xml"):
            sample_xml_to_csv(xml_file_path, csv_file_path)
        if file.endswith("experiment.xml"):
            experiment_xml_to_csv(xml_file_path, csv_file_path)
        if file.endswith("study.xml"):
            study_xml_to_csv(xml_file_path, csv_file_path)
        if file.endswith("experiment.xml"):
            experiment_xml_to_csv(xml_file_path, csv_file_path)
        if file.endswith("run.xml"):
            run_xml_to_csv(xml_file_path, csv_file_path)
        