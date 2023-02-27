"""
todo: cleaningAndAddingSeenData() and cleaningAndAddingUnseenData() need to be made more efficient
- downloads fresh copy of the iMDb database (if var set)
- updates /data/handcrafted/raw_status.xslsx with movies in the two files:
    - /data/handcrafted/add_movies_seen.txt
    - - /data/handcrafted/add_movies_unseen.txt
"""


import os
import re
import time
import gzip
import shutil
import requests
import numpy as np
import pandas as pd
from datetime import date

START_TIME = time.time()

BASE_URL = "https://datasets.imdbws.com/"

FILES_IMDB = {
    "tit_bas": "title.basics.tsv",
    "tit_rate": "title.ratings.tsv",
    "name_bas": "name.basics.tsv",
    "tit_prin": "title.principals.tsv",
    "cast_crew": "title.crew.tsv",
}

FILES_HAND = {
    "add_seen": "add_movies_seen.txt",
    "add_unseen": "add_movies_unseen.txt",
    "add_secop": "add_movies_second_opinion.txt",
    "raw_status": "raw_status.xlsx"
}

FILES_GENERATED = {
    "films_raw": "films_raw.pkl"
}

# set this var if you want to downoad a fresh copy of the latest iMDb movie data
DOWNLOAD = True

def main():
    # 1 download the files
    if DOWNLOAD:
        downloadFiles()
    
    # 2 update the watched list
    seen_path = os.path.join("data", "handcrafted", FILES_HAND["add_seen"])
    seen_raw_f = open(seen_path,'r')
    seen_raw = seen_raw_f.readlines()

    unseen_path = os.path.join("data", "handcrafted", FILES_HAND["add_unseen"])
    unseen_raw_f = open(unseen_path,'r')
    unseen_raw = unseen_raw_f.readlines()

    ids_and_status = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    movie_list_raw = pd.read_excel(ids_and_status)
    movie_list_raw = updateWatchList(seen_raw, unseen_raw, movie_list_raw)
    movie_list_raw.sort_values(["tconst"]).to_excel(ids_and_status, index=False)


def downloadFiles():
    """ Downloads needed files and removes old files if already excist"""

    if not os.path.exists("data/imdb"):
        os.makedirs("data/imdb")
    
    for file in FILES_IMDB.values():
        file_name = os.path.join("data/imdb/",file)
        file_zip = file_name+".gz"
        file_url = BASE_URL+file+".gz"
        
        # remove old files
        if os.path.exists(file_name):
            os.remove(file_name)
        if os.path.exists(file_zip):
            os.remove(file_zip)
        
        # download files
        response = requests.get(file_url)
        open(file_zip , "wb").write(response.content)

        # unzip files
        with gzip.open(file_zip, 'rb') as f_in:
            with open(file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # remove zips
        os.remove(file_zip)


def updateWatchList(seen_raw, unseen_raw, movie_list_raw):
    ## getting the new seen movies
    ##############################

    # file lines for seen movies must be one of the following:
    #   <url>|<score as float>|<date as day-month-year>
    #   <url>|<score as float>
    #   <url>||<date as day-month-year>
    #   <url>|

    # transforming the new seen movie data
    if seen_raw != []:
        # cleaning the seen data
        movie_list_raw = cleaningAndAddingSeenData(seen_raw, movie_list_raw)

    ## getting the new unseen movies
    ################################
    if unseen_raw != []:
        # transforming the new unseen movie data
        movie_list_raw = cleaningAndAddingUnseenData(unseen_raw, movie_list_raw)
        
    return movie_list_raw


def cleaningAndAddingSeenData(seen_raw, movie_list_raw):
    # todo: the two for loops can be joined
    # cleaning the seen data
    for linei in range(len(seen_raw)):
        seen_raw[linei] = seen_raw[linei].strip().split("|")
        # getting the imdb code
        url = seen_raw[linei][0].split("/")
        # temp = seen etc..
        tofind = re.compile("^tt\d+\d$")
        ttcode = ""
        for x in url:
            y = tofind.findall(x)
            if len(list(y)) != 0:
                ttcode = list(y)[0]
        
        if len(seen_raw[linei]) == 1:
            # only url
            seen_raw[linei] = [ttcode, None, None]
        elif len(seen_raw[linei]) == 2:
            # url and score
            seen_raw[linei] = [ttcode, float(seen_raw[linei][1]), None]
        elif len(seen_raw[linei]) == 3:
            # url and date, possibly also a score
            row2 = seen_raw[linei][1]
            row3 = seen_raw[linei][2]
            if row2 == '':
                row2 = None
            else:
                row2 = float(row2)
            filmdate = row3.split("-")
            filmdate = date(int(filmdate[2]), int(filmdate[1]), int(filmdate[0]))
            print("in modding loop",filmdate)
            seen_raw[linei] = [ttcode, row2, filmdate]
    
    # adding the seen data
    for seen in seen_raw:
        movieid = seen[0]
        score = seen[1]
        seen_raw_date = seen[2]
        if movieid in movie_list_raw["tconst"].values:
            # if watched movie already in list
            found_index = movie_list_raw.loc[movie_list_raw.loc[:,"tconst"]==movieid].index.tolist()[0]
            movie_list_raw.at[found_index,"enjoyment"]
            enjoyment = movie_list_raw.at[found_index,"enjoyment"]
            watched = int(movie_list_raw.at[found_index,"watched"])
            if watched==1:
                print("in the watched==1")
                # update the score only if null
                if(pd.isnull(enjoyment)):
                    movie_list_raw.at[found_index,"enjoyment"] = score
                # update the date only if new data is not null
                if(seen_raw_date != None):
                    movie_list_raw.at[found_index,"watched_date"] = seen_raw_date
            elif watched==0:
                # updated watched and add score (which can be nan)
                movie_list_raw.at[found_index,"enjoyment"] = score
                movie_list_raw.at[found_index,"watched"] = 1
        else:
            # if watched movie not in list
            to_add = pd.Series({
                'tconst':movieid, 'watched':1, 'netflix':np.nan,
                'prime':np.nan, "enjoyment":score , "priority": np.nan, "watched_date": seen_raw_date})

            movie_list_raw = pd.concat([movie_list_raw, to_add.to_frame().T], ignore_index=True)
    
    return movie_list_raw


def cleaningAndAddingUnseenData(unseen_raw, movie_list_raw):
    # todo: the two for loops can be joined
    for linei in range(len(unseen_raw)):
        unseen_raw[linei] = unseen_raw[linei].strip()
        temp = unseen_raw[linei].split("/")
        
        tofind = re.compile("^tt\d+\d$")
        ttcode = ""
        for x in temp:
            y = tofind.findall(x)
            if len(list(y)) != 0:
                ttcode = list(y)[0]
        unseen_raw[linei] = ttcode

    # adding the unseen data
    for movieid in unseen_raw:
        if not movieid in movie_list_raw["tconst"].values:
            # if watched movie not already in list
            to_add = pd.Series({
                'tconst':movieid, 'watched':0, 'netflix':np.nan,
                'prime':np.nan, "enjoyment":np.nan , "priority": np.nan})

            movie_list_raw = pd.concat([movie_list_raw, to_add.to_frame().T], ignore_index=True)
    
    return movie_list_raw

if __name__ == "__main__":
    main()