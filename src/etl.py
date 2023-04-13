import requests
import shutil
import os
import re
from time import time

class etl:
    def __init__(self):
        self.data_source = 'https://dadosabertos.rfb.gov.br/CNPJ'

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

if __name__ == '__main__':
    driver = etl()
    driver.delete_files('./files/backup')