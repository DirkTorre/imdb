"""
- makes excel file for keeping track of watched movies
- makes file for dataminging
todo:
fix createHumanReadableFile() (i modded the data)
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
    "films_raw": "films_raw.pkl",
    "films_reading": "films_reading.xlsx"
}


def main():
    # 1 load and clean the watched movies data
    ids_and_status = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    watched = pd.read_excel(ids_and_status)
    watched = CleanWatchedData(watched)

    # 2 load the files that are needed to extend the watched movies
    title_basics_path = os.path.join("data", "imdb", FILES_IMDB["tit_bas"])
    title_basics = pd.read_csv(title_basics_path, sep="\t", low_memory=False)
    title_rate_path = os.path.join("data", "imdb", FILES_IMDB["tit_rate"])
    title_rate = pd.read_csv(title_rate_path, sep="\t")
    name_basics_path = os.path.join("data", "imdb", FILES_IMDB["name_bas"])
    name_basics = pd.read_csv(name_basics_path, sep="\t", low_memory=False)
    title_crew_path = os.path.join("data", "imdb", FILES_IMDB["cast_crew"])
    title_crew = pd.read_csv(title_crew_path, sep="\t", low_memory=False)
    title_prin_mega_path = os.path.join("data", "imdb", FILES_IMDB["tit_prin"])

    # 3 add all info
    watched = addIMDbData(watched, title_basics, title_rate, title_prin_mega_path, title_crew, name_basics)

    # this one does not work annymore, because i modded the script, needs to be fixed
    # 4 create a human readable file and save it
    output_path = os.path.join("data", "generated", FILES_GENERATED["films_reading"])
    createHumanReadableFile(watched, output_path)

    # 5 save dataset for mining
    output_path = os.path.join("data", "generated", FILES_GENERATED["films_raw"])
    saveAsPickle(watched, output_path)

    # 6 making the dataset ready for mining and ML will be done in a different script


def CleanWatchedData(watched):
    watched["watched"] = watched["watched"].astype('Int64').astype(bool)
    watched["prime"] = watched["prime"].astype('Int64').replace(0, False).replace(1, True)
    watched["netflix"] = watched["netflix"].astype('Int64').replace(0, False).replace(1, True)
    watched["enjoyment"] = watched["enjoyment"].astype('float')
    watched["tconst"] = watched["tconst"].str.strip()
    return watched


def addIMDbData(watched, title_basics, title_rate, title_prin_mega_path, title_crew, name_basics):
    watched = addBasicTitleData(watched, title_basics)
    watched = addRatings(watched, title_rate)
    watched = addCastAndCrew(watched, title_prin_mega_path, title_crew, name_basics)
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


def addCastAndCrew(watched_title_rate, title_prin_mega_path, title_crew, name_basics):
    # get cast and crew from the big file
    watched_tconst = watched_title_rate.loc[:,"tconst"].to_frame()
    watched_films_cast = pd.DataFrame(columns=["tconst", "ordering","nconst", "category", "job", "characters"])
    counter = 1
    for chunk in pd.read_csv(title_prin_mega_path, sep="\t", chunksize=100000):
        rows = pd.merge(watched_tconst.loc[:,"tconst"], chunk, on="tconst", how="inner")
        watched_films_cast = pd.concat([rows,watched_films_cast], ignore_index = True)
        print("chunk #",counter)
        counter += 1
    watched_films_cast.replace(to_replace = "\\N", value = np.nan, inplace=True)
    watched_films_cast.drop(['characters'], axis=1, inplace=True)
    
    # split fetched cast and crew on values [director, writer] and other
    direct = watched_films_cast.category == 'director'
    writer = watched_films_cast.category == 'writer'
    both = direct | writer
    # this isn't needed yet, takes a lot of space
    principals_writer_direct = watched_films_cast.loc[both,:]
    principals_other = watched_films_cast.loc[~both,:]
    
    # get the directors and writers for our movie id's (tconst); reformat and add them together

    title_crew.replace(to_replace = "\\N", value = np.nan, inplace=True)

    directors = title_crew.loc[~title_crew.loc[:,'directors'].isna(),['tconst','directors']]
    needed_directors = pd.merge(watched_tconst.loc[:,"tconst"], directors, on="tconst", how="left")
    needed_directors.directors = needed_directors.directors.str.split(',')
    needed_directors = needed_directors.explode('directors')
    needed_directors.loc[:,'category'] = 'director'
    needed_directors.rename(columns={"directors": "nconst"}, inplace=True)

    writers = title_crew.loc[~title_crew.loc[:,'writers'].isna(),['tconst','writers']]
    needed_writers = pd.merge(watched_tconst.loc[:,"tconst"], writers, on="tconst", how="left")
    needed_writers.writers = needed_writers.writers.str.split(',')
    needed_writers = needed_writers.explode('writers')
    needed_writers.loc[:,'category'] = 'writer'
    needed_writers.rename(columns={"writers": "nconst"}, inplace=True)

    needed_directors_writers = pd.concat([needed_directors, needed_writers], ignore_index=True)

    # now take needed_directors_writers and fill it with the ordering and job from principals_writer_direct.
    needed_dir_writ_principals = pd.merge(needed_directors_writers, principals_writer_direct, on=["tconst",'nconst'], how="left")
    needed_dir_writ_principals = needed_dir_writ_principals.drop('category_y', axis=1)
    needed_dir_writ_principals.rename(columns={'category_x': "category"}, inplace=True)

    # add the non-director / writer principials
    principals_better = pd.concat([principals_other, needed_dir_writ_principals],ignore_index=True)

    # add name_basics
    principals_better = pd.merge(principals_better, name_basics, on=['nconst'], how="left")

    # clean data now it still isn't that much
    principals_better.ordering = principals_better.ordering.astype("Int64")
    principals_better = principals_better.replace(to_replace = "\\N", value = np.nan)
    principals_better.birthYear = principals_better.birthYear.astype("Int64")
    principals_better.deathYear = principals_better.deathYear.astype("Int64")
    principals_better.primaryProfession = principals_better.primaryProfession.str.split(',')
    principals_better.knownForTitles = principals_better.knownForTitles.str.split(',')

    # explode the data
    principals_better = principals_better.explode('primaryProfession')
    principals_better = principals_better.explode('knownForTitles')

    # join data
    fin = pd.merge(watched_title_rate, principals_better, how='left', on="tconst")

    return fin

    # # add cast and crew
    # watched_title_rate_personel = pd.merge(watched_title_rate, watched_films_cast, how='inner', on='tconst')
    # del watched_title_rate
    # del watched_films_cast

    # # add info about personell
    # names_file = os.path.join("data", "imdb", FILES_IMDB["name_bas"])
    # names_basics = pd.read_csv(names_file, sep="\t")
    # names_basics = names_basics.replace(to_replace = "\\N", value = np.nan)
    # names_basics.loc[:,"birthYear"] = names_basics.loc[:,"birthYear"].astype('Int64')
    # names_basics.loc[:,"deathYear"] = names_basics.loc[:,"deathYear"].astype('Int64')
    # col_delete = ["knownForTitles"]
    # names_basics = names_basics.drop(col_delete, axis=1)
    
    # # join data
    # watched_title_rate_personel_names = pd.merge(watched_title_rate_personel, names_basics, how='left', on="nconst")
    # del watched_title_rate_personel


def saveAsPickle(watched, output):
        watched.to_pickle(output)

        END_TIME = time.time()
        time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
        print("Execution time: ",time_format)


def createHumanReadableFile(raw_film_data, output_path):
    # reformat staff age data into 1 column and make columns from the categories
    staff = raw_film_data.loc[:,['tconst','nconst','category','primaryName','birthYear','deathYear']].drop_duplicates()
    staff.loc[:,'printname'] = staff.loc[:,'primaryName'] + " " + staff.loc[:,'birthYear'].astype('str') + ' - ' + staff.loc[:,'deathYear'].astype(str)
    staff.drop(['nconst','primaryName','birthYear','deathYear'], axis=1, inplace=True)
    staff = staff.groupby(['tconst','category'])['printname'].aggregate(lambda x: tuple(x)).unstack()
    staff.drop(['archive_footage','self','production_designer'], axis=1, inplace=True)

    # make all genres into columns for easy filtering in excel
    genres = raw_film_data.loc[:,['tconst','genres']].drop_duplicates()
    genres.genres = genres.genres.str.split(',')
    genres = genres.explode('genres')
    genres['value'] = 1
    genres = pd.pivot_table(genres.explode('genres'), values='value', index='tconst', columns='genres', fill_value=0)

    # remove old data and add new data to the movies
    readable_data = raw_film_data.copy()
    readable_data.drop(['isAdult','ordering', 'nconst','category','job','primaryName','birthYear','deathYear', 'genres','endYear','primaryProfession', 'knownForTitles'], axis=1, inplace=True)
    readable_data.drop_duplicates(inplace=True)
    readable_data = pd.merge(readable_data, genres, on="tconst", how="left")
    readable_data = pd.merge(readable_data, staff, on="tconst", how="left")
    readable_data.loc[:,'watched'] = readable_data.loc[:,'watched'].replace(True,1).replace(False,0)
    readable_data

    readable_data.sort_values(by=['watched', 'averageRating'], ascending=False).to_excel(output_path, index=False)



if __name__ == "__main__":
    main()