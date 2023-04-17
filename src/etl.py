import findspark
import requests
import pathlib
import shutil
import json
import os
from bs4 import BeautifulSoup
from zipfile import ZipFile
from pyspark.sql import SparkSession
src_dir = str(pathlib.Path(__file__).parent.resolve())
findspark.init()

class etl:
    def __init__(self, labels_language: str = 'en'):
        self.data_source = 'https://dadosabertos.rfb.gov.br/CNPJ'
        with open(os.path.join(src_dir,'layout.json'), mode='r') as f:
            self.tables_layout = json.load(f)
            f.close()
        
    def init_spark(self, master: str='local[*]'):
        self.spark = SparkSession.Builder().appName('brazil-legal-entities')\
                        .enableHiveSupport().master(master=master).getOrCreate()
    
    def stop_spark(self):
        self.spark.stop()
        del self.spark   
    
    def delete_files(self, folder_path: str):
        for file_path in os.scandir(folder_path):
            print(file_path)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
    
    def switch_folders_names(self, dir: str, folder1: str, folder2: str):
        folder1_path = os.path.join(dir,folder1)
        folder2_path = os.path.join(dir,folder2)
        placeholder_path = os.path.join(dir,'placeholder')
        os.rename(folder1_path, placeholder_path)
        os.rename(folder2_path, folder1_path)
        os.rename(placeholder_path, folder2_path)

    def fetch_filenames(self):
        source_html = requests.get(self.data_source).text
        soup = BeautifulSoup(source_html,'lxml')
        self.filenames = [row.get('href') for row in soup.find_all('a')[5:-1]]

    def download(self, url: str, to_folder: str='./files/raw'):
        local_filename = url.split('/')[-1]
        with requests.get(url, stream=True) as r:
            content_len = float(r.headers['Content-Length'])
            progress_len = 0
            r.raise_for_status()
            with open(to_folder+'/'+local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    progress_len+=len(chunk)
                    print(f'\r File {local_filename}: downloaded {format(progress_len/1000000,".2f")}MB out of {format(content_len/1000000,".2f")}MB ({format(100*progress_len/content_len,".2f")}%).', end='')
                f.close()
                print('')
    
    def download_all(self):
        self.fetch_filenames()
        self.delete_files('./files/backup')
        self.switch_folders_names('./files','raw','backup')
        len_filenames = len(self.filenames)
        try:
            for index, filename in enumerate(self.filenames):
                print(f'\nFile {index+1} of {len_filenames}.')
                self.download(url=os.path.join(self.data_source,filename))
            self.delete_files('./files/backup')
            print('\nDownload completed sucessfuly!')
        except:
            print('\nDownload failed. Aborting...')
            self.delete_files('./files/raw')
            self.switch_folders_names('./files','raw','backup')

    def unzip(self, file_path: str, to_folder: str):
        with ZipFile(file_path, mode='r') as file:
            file.extractall(to_folder)
            file.close()
    
    def unzip_all(self):
        self.delete_files('./files/temp')
        ziped_files = [
            filename for filename in os.listdir('./files/raw') if filename[-4:]=='.zip'
            ]
        for key in self.tables_layout:
            print(f"Extracting parts for '{key}' file.")
            to_folder = os.path.join('./files/temp', key.lower())
            for filename in [
                filename for filename in ziped_files if key.upper() in filename.upper()
                ]:
                file_path = os.path.join('./files/raw', filename)
                self.unzip(file_path=file_path, to_folder=to_folder)

if __name__ == '__main__':
    driver = etl()
    driver.unzip_all()