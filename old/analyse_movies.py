import requests
import gzip
import shutil
import os

base_url = "https://datasets.imdbws.com/"
title_basics_file = "title.basics.tsv"
title_basics_zip = "title.basics.tsv.gz"

BASE_URL = "https://datasets.imdbws.com/"
FILES = {
    "tit_bas_f": "title.basics.tsv",
    "tit_bas_z": "title.basics.tsv.gz"
    }

DOWNLOAD = True

def main():
    if DOWNLOAD:
        getFiles()    


def getFiles():
    # remove if already excist
    if not os.path.exists("data"):
        os.makedirs("data")
    if os.path.exists("data/"+FILES["tit_bas_f"]):
        os.remove("data/"+FILES["tit_bas_f"])
    if os.path.exists("data/"+FILES["tit_bas_z"]):
        os.remove("data/"+FILES["tit_bas_z"])
    
    
    # download files
    response = requests.get(BASE_URL+FILES["tit_bas_z"])
    open("data/"+FILES["tit_bas_z"], "wb").write(response.content)

    # unzip files
    with gzip.open("data/"+FILES["tit_bas_z"], 'rb') as f_in:
        with open("data/"+FILES["tit_bas_f"], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # remove zips
    os.remove("data/"+FILES["tit_bas_z"])

main()