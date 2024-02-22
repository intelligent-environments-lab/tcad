import requests
import warnings

from pathlib import Path
import subprocess


def _download(url=None,headers=None):
    """Deprecated: please download and extract manually"""
    DEFAULT_URL = "https://traviscad.org/wp-content/largefiles/2023%20Certified%20Appraisal%20Export%20Supp%200_07232022.zip"
    DEFAULT_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    }
    url = url or DEFAULT_URL
    headers = headers or DEFAULT_HEADERS

    Path('.cache').mkdir(parents=True,exist_ok=True)
    
    filepath = f'.cache/{url.split("/")[-1].replace("%20","_")}'

    data = requests.get(url,headers=headers).content
    with open(filepath,'wb') as f:
        f.write(data)
    
    def extract():
        try:
            path_7zip = r"C:/Program Files/7-Zip/7z.exe"
            command = [path_7zip,'x',filepath,'-o'+filepath.replace('.zip',''),'-aos']
            subprocess.check_output(command)
        except:
            warnings.warn("Unable to extract with 7zip. Please extract the zip file in .cache manually.")

    extract()
