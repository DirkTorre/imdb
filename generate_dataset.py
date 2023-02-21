"""
- makes excel file for keeping track of watched movies
- makes file for dataminging
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
    "cast_crew": "title.principals.tsv",
}

FILES_HAND = {
    "add_seen": "add_movies_seen.txt",
    "add_unseen": "add_movies_unseen.txt",
    "add_secop": "add_movies_second_opinion.txt",
    "raw_status": "raw_status.xlsx"
}

FILES_GENERATED = {
    "films_raw": "films_raw.pkl",
    "films_reading": "films_reading.xlsx"
}

# set this var if you want to downoad a fresh copy of the latest iMDb movie data
DOWNLOAD = False


def main():
    # 1 load and clean the watched movies data
    ids_and_status = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    watched = pd.read_excel(ids_and_status)
    watched = loadAndCleanWatchedData(watched)

    # 2 load the files that are needed to extend the watched movies
    title_basics_path = os.path.join("data", "imdb", FILES_IMDB["tit_bas"])
    title_basics = pd.read_csv(title_basics_path, sep="\t", low_memory=False)
    title_rate_path = os.path.join("data", "imdb", FILES_IMDB["tit_rate"])
    title_rate = pd.read_csv(title_rate_path, sep="\t")
    cast_crew_mega_path = os.path.join("data", "imdb", FILES_IMDB["cast_crew"])

    # 3 add all info
    watched = addIMDbData(watched, title_basics, title_rate, cast_crew_mega_path)

    # 4 create a human readable file and save it
    output_path = os.path.join("data", "generated", FILES_GENERATED["films_reading"])
    createHumanReadableFile(watched, output_path)

    # 5 save dataset for mining
    output_path = os.path.join("data", "generated", FILES_GENERATED["films_raw"])
    saveAsPickle(watched, output_path)

    # 6 making the dataset ready for mining and ML will be done in a different script


def loadAndCleanWatchedData(watched):
    watched["watched"] = watched["watched"].astype('Int64').astype(bool)
    watched["prime"] = watched["prime"].astype('Int64').replace(0, False).replace(1, True)
    watched["netflix"] = watched["netflix"].astype('Int64').replace(0, False).replace(1, True)
    watched["enjoyment"] = watched["enjoyment"].astype('float')
    watched["tconst"] = watched["tconst"].str.strip()
    return watched


def addIMDbData(watched, title_basics, title_rate, cast_crew_mega_file):
    watched = addBasicTitleData(watched, title_basics)
    watched = addRatings(watched, title_rate)
    watched = addCastAndCrew(watched, cast_crew_mega_file)
    return watched


def addBasicTitleData(watched, title_basics):
    title_basics = title_basics.replace(to_replace = "\\N", value = np.nan)
    watched_title = pd.merge(watched, title_basics, on="tconst", how="left")
    del title_basics # cleanup memory by force, possibly not needed now because no longer using jupyter notebook
    return watched_title


def addRatings(watched_title, title_rate):
    title_rate = title_rate.replace(to_replace = "\\N", value = np.nan)
    title_rate.loc[:,"numVotes"] = title_rate.loc[:,"numVotes"].astype('Int64')
    watched_title_rate = pd.merge(watched_title,title_rate, on="tconst", how="left")
    del watched_title # cleanup memory by force
    return watched_title_rate


def addCastAndCrew(watched_title_rate, cast_crew_mega_file):
    # get cast and crew from the big file
    watched_films_cast  = pd.DataFrame(columns=["tconst", "ordering","nconst", "category", "job", "characters"])
    counter = 1
    for chunk in pd.read_csv(cast_crew_mega_file, sep="\t", chunksize=100000):
        rows = pd.merge(watched_title_rate.loc[:,"tconst"], chunk, on="tconst", how="inner")
        watched_films_cast = pd.concat([rows,watched_films_cast], ignore_index = True)
        print("chunk #",counter)
        counter += 1
    watched_films_cast.replace(to_replace = "\\N", value = np.nan, inplace=True)
    watched_films_cast.drop(['characters'], axis=1, inplace=True)
    
    # add cast and crew
    watched_title_rate_personel = pd.merge(watched_title_rate, watched_films_cast, how='inner', on='tconst')
    del watched_title_rate
    del watched_films_cast

    # add info about personell
    names_file = os.path.join("data", "imdb", FILES_IMDB["name_bas"])
    names_basics = pd.read_csv(names_file, sep="\t")
    names_basics = names_basics.replace(to_replace = "\\N", value = np.nan)
    names_basics.loc[:,"birthYear"] = names_basics.loc[:,"birthYear"].astype('Int64')
    names_basics.loc[:,"deathYear"] = names_basics.loc[:,"deathYear"].astype('Int64')
    col_delete = ["knownForTitles"]
    names_basics = names_basics.drop(col_delete, axis=1)
    
    # join data
    watched_title_rate_personel_names = pd.merge(watched_title_rate_personel, names_basics, how='left', on="nconst")
    del watched_title_rate_personel
    return watched_title_rate_personel_names


def saveAsPickle(watched, output):
        watched.to_pickle(output)

        END_TIME = time.time()
        time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
        print("Execution time: ",time_format)


def createHumanReadableFile(raw_film_data, output_path):
    # reformat staff data into 1 colummn
    staff = raw_film_data.loc[:,['tconst','nconst','category','primaryName','birthYear','deathYear']]
    staff.loc[:,'printname'] = staff.loc[:,'primaryName'] + " " + staff.loc[:,'birthYear'].astype('str') + ' - ' + staff.loc[:,'deathYear'].astype(str)
    staff.drop(['nconst','primaryName','birthYear','deathYear'], axis=1, inplace=True)
    staff = staff.groupby(['tconst','category'])['printname'].aggregate(lambda x: tuple(x)).unstack()

    # make all genres into columns for easy filtering in excel
    genres = raw_film_data.loc[:,['tconst','genres']].drop_duplicates()
    genres.genres = genres.genres.str.split(',')
    genres = genres.explode('genres')
    genres['value'] = 1
    genres = pd.pivot_table(genres.explode('genres'), values='value', index='tconst', columns='genres', fill_value=0)

    # remove old data and add new data to the movies
    readable_data = raw_film_data.copy()
    readable_data.drop(['isAdult','ordering', 'nconst','category','job','primaryName','birthYear','deathYear', 'genres','endYear','primaryProfession'], axis=1, inplace=True)
    readable_data.drop_duplicates(inplace=True)
    readable_data = pd.merge(readable_data, genres, on="tconst", how="left")
    readable_data = pd.merge(readable_data, staff, on="tconst", how="left")
    readable_data.loc[:,'watched'] = readable_data.loc[:,'watched'].replace(True,1).replace(False,0)
    
    readable_data.sort_values(by=['watched', 'averageRating'], ascending=False).to_excel(output_path, index=False)
    


if __name__ == "__main__":
    main()