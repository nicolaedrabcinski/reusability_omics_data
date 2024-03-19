import os
import pandas as pd
import requests
import ftplib
import tarfile

from io import StringIO
from tqdm import tqdm

local_dir_temp_samples = '../data/interim/geo_samples'
local_dir_temp_series = '../data/interim/geo_series'
local_dir_temp_platforms = '../data/interim/geo_platforms'

output_file_samples = '../data/processed/geo_samples.csv'
output_file_series = '../data/processed/geo_series.csv'
output_file_platforms = '../data/processed/geo_platforms.csv'

URL_GEO_SAMPLES = 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=samples&sort=date&mode=csv&page={}&display=5000'
URL_GEO_SERIES = 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=series&sort=date&mode=csv&page={}&display=5000'
URL_GEO_PLATFORMS = 'https://www.ncbi.nlm.nih.gov/geo/browse/?view=platforms&sort=date&mode=csv&page={}&display=5000'
URL_SRA_RUNS = 'https://ftp-trace.ncbi.nlm.nih.gov/sra/reports/Metadata/'

samples_page_number = 1
series_page_number = 1
platforms_page_number = 1

desired_samples_header = "Accession,Title,Sample Type,Taxonomy,Channels,Platform,Series,Supplementary Types,Supplementary Links,SRA Accession,Contact,Release Date"
desired_series_header = "Accession,Title,Series Type,Taxonomy,Sample Count,Datasets,Supplementary Types,Supplementary Links,PubMed ID,SRA Accession,Contact,Release Date"
desired_platforms_header = "Accession,Title,Technology,Taxonomy,Data Rows,Samples Count,Series Count,Contact,Release Date"

while True:
    url = URL_GEO_SAMPLES.format(page_number)
    response = requests.get(url)
    
    if response.status_code == 200:
       
        if response.text.strip() == desired_samples_header:
            print("No more data available. Stopping.")
            break

        temp_df = pd.read_csv(StringIO(response.text))
        temp_csvfile_path = os.path.join(local_dir_temp, f'samples_{samples_page_number}.csv')
        temp_df.to_csv(temp_csvfile_path, index=False)
        
        print("Processing pages: ", samples_page_number)
        page_number += 1
    else:
        print(f"Failed to fetch data from page {samples_page_number}")
        break

csv_files = [f for f in os.listdir(local_dir_temp_samples) if f.endswith('.csv')]
combined_df = pd.DataFrame()

for csv_file in csv_files:
    file_path = os.path.join(local_dir_temp, csv_file)
    temp_df = pd.read_csv(file_path)
    combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

output_file_path = os.path.join(output_dir, 'geo_samples.csv')
combined_df.to_csv(output_file_path, index=False)

while True:
    url = URL_GEO_SERIES.format(series_page_number)
    response = requests.get(url)

    if response.status_code == 200:
        if response.text.strip() == desired_series_header:
        # temp_df = pd.read_csv(pd.compat.StringIO(response.text))
            print('No more data available. Stopping.')
            break
    
        temp_df = pd.read_csv(StringIO(response.text))
        temp_csvfile_path = os.path.join(local_dir_temp_series, f'series_{series_page_number}.csv')
        temp_df.to_csv(temp_csvfile_path, index=False)

        print("Processing pages: ", series_page_number)
        series_page_number += 1
    
    else:
        print(f"Failed to fetch data from page {series_page_number}")
        break

csv_files = [f for f in os.listdir(local_dir_temp_series) if f.endswith('.csv')]
combined_df = pd.DataFrame()

for csv_file in csv_files:
    file_path = os.path.join(local_dir_temp_series, csv_file)
    temp_df = pd.read_csv(file_path)
    combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

combined_df.to_csv(output_file_series, index=False)

while True:
    url = URL_GEO_PLATFORMS.format(platforms_page_number)
    response = requests.get(url)

    if response.status_code == 200:
        if response.text.strip() == desired_platforms_header:
            print('No more data available. Stopping')
            break

        temp_df = pd.read_csv(StringIO(response.text))
        temp_csvfile_path = os.path.join(local_dir_temp_platforms, f'platforms_{platforms_page_number}.csv')
        temp_df.to_csv(temp_csvfile_path, index=False)

        print("Processing pages: ", platforms_page_number)
        platforms_page_number += 1

    else:
        print(f"Failed to fetch data from page {platforms_page_number}")

csv_files = [f for f in os.listdir(local_dir_temp_platforms) if f.endswith('.csv')]
combined_df = pd.DataFrame()

for csv_file in csv_files:
    file_path = os.path.join(local_dir_temp_platforms, csv_file)
    temp_df = pd.read_csv(file_path)
    combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

combined_df.to_csv(output_file_platforms, index=False)



