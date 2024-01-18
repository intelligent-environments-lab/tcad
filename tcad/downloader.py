import os
import requests
import warnings

from pathlib import Path
import subprocess

url = "https://traviscad.org/wp-content/largefiles/2023%20Certified%20Appraisal%20Export%20Supp%200_07232022.zip"
filename = f'.cache/{url.split("/")[-1].replace("%20","_")}'
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    }

def download():
    Path('.cache').mkdir(parents=True,exist_ok=True)
    
    data = requests.get(url,headers=headers).content
    with open(filename,'wb') as f:
        f.write(data)

    
def extract():
    try:
        path_7zip = r"C:/Program Files/7-Zip/7z.exe"
        command = [path_7zip,'x',filename,'-o'+filename.replace('.zip',''),'-aos']
        subprocess.check_output(command)
    except:
        warnings.warn("Unable to extract with 7zip. Please extract the zip file in .cache manually.")

if __name__ == '__main__':
    tcad_dir = download_tcad_data(url)