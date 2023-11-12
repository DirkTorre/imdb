# There is a problem when there are duplicate tconst in the to add list

import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import date
from datetime import timedelta

START_TIME = time.time()

BASE_URL = "https://datasets.imdbws.com/"
PARQ_PATH = "data/imdb/parquet/"
DOWNLOAD_PATH = "data/imdb/download/"
OUTPUT_PATH = "data/generated/"


FILES_IMDB = {
    "cast_crew" : "title.crew.tsv",
    "tit_bas" : "title.basics.tsv",
    "tit_rate" : "title.ratings.tsv",
    "name_bas" : "name.basics.tsv",
    "tit_prin" : "title.principals.tsv",
}

FILES_IMDB_PARQ = {
    "tit_bas" : "title_basics.parquet",
    "genres" : "genres.parquet",
    "tit_rate" : "title_ratings.parquet",
    'directors' : 'directors.parquet',
    'writers' : 'writers.parquet',
    'prim_prof' : 'primary_profession.parquet',
    'known_for' : 'known_for_titles.parquet',
    'name_bas' : 'name_basics.parquet',
    'const' : 'ids.parquet',
    'ordering' : 'ordering.parquet',
    'character' : 'character.parquet',
    'job' : 'job.parquet',
}

FILES_HAND = {
    "add_seen": "add_movies_seen.txt", # old
    "add_unseen": "add_movies_unseen.txt", # old
    "add_secop": "add_movies_second_opinion.txt", # old
    "raw_status": "raw_status.xlsx",
    "to_add" : "to_add.xlsx"
}

FILES_GENERATED = {
    "films_raw": "films_raw.pkl",
    "films_reading": "films_reading.xlsx"
}


def main():
    # load and update data
    to_add, raw_stat = loadData()
    changed_rows, raw_stat = addNewAndGetChanged(to_add, raw_stat)
    raw_stat = updateIndexedMovies(changed_rows, raw_stat)

    # overwrite raw_status
    output = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    raw_stat.sort_index().to_excel(output)
    
    # empty to_add.xlsx
    new_empty = pd.DataFrame(data=None, columns=["link"]+to_add.columns.to_list())
    to_add = os.path.join("data", "handcrafted", FILES_HAND["to_add"])
    new_empty.to_excel(to_add, index=False)


def setAttr(frame):
    # setting column types
    frame['watched_date'] = pd.to_datetime(frame['watched_date'])
    frame[['enjoyment','story','subject','acting','visual','action','comedy']] = frame[['enjoyment','story','subject','acting','visual','action','comedy']].astype(float)
    frame['watched'] = frame['watched'].astype("Int64").replace(0,np.nan)
    frame[['netflix','prime','priority']] = frame[['netflix','prime','priority']].astype("Int64")
    frame = frame.drop_duplicates()
    return frame


def loadData():
    """Loads the raw excel files."""
    # loading and preparing films to add
    id_stat = os.path.join("data", "handcrafted", FILES_HAND["to_add"])
    to_add = pd.read_excel(id_stat)
    to_add = setAttr(to_add)
    to_add['link'] = to_add['link'].str.split("/",expand=True).loc[:,4].astype(str)
    to_add = to_add.rename(columns={"link":"tconst"}).set_index("tconst")

    # loading and preparing film list
    raw_stat_link = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    raw_stat = pd.read_excel(raw_stat_link)
    raw_stat = setAttr(raw_stat)
    raw_stat = raw_stat.set_index("tconst")

    return to_add, raw_stat


def addNewAndGetChanged(to_add, raw_stat):
    """Adds new movies to raw_stat, and get movies that need to be updated."""
    # adding new films and creating a subset of films to update
    direct_toevoegen = to_add[~to_add.index.isin(raw_stat.index)]
    door_scanner = to_add[to_add.index.isin(raw_stat.index)]
    raw_stat = pd.concat([raw_stat, direct_toevoegen])

    # preparing data for comparison
    door_scanner = door_scanner.fillna(-1)
    door_scanner.loc[:,"watched"] = door_scanner.loc[:,"watched"].replace(-1,0)
    door_scanner.loc[:,"watched_date"] = door_scanner.loc[:,"watched_date"].replace(-1,pd.to_datetime("1/1/1900"))

    raw_stat = raw_stat.fillna(-1)
    raw_stat.loc[:,"watched"] = raw_stat.loc[:,"watched"].replace(-1,0)
    raw_stat.loc[:,"watched_date"] = raw_stat.loc[:,"watched_date"].replace(-1,pd.to_datetime("1/1/1900"))

    # get the movies for adding that are already in the movie list
    identical_rows = pd.merge(door_scanner.reset_index(drop=False),
                            raw_stat.reset_index(drop=False),
                            on=door_scanner.reset_index(drop=False).columns.values.tolist(),
                            how='inner')['tconst']
    changed_rows = door_scanner[~door_scanner.index.isin(identical_rows)]

    return changed_rows, raw_stat

def updateIndexedMovies(changed_rows, raw_stat):
    # Update movie info
    for index, row in changed_rows.iterrows():
        # if movie is watched, also make it watched in original list
        if changed_rows.loc[index,"watched"] == 1:
            raw_stat.loc[index,"watched"] = 1
        # if watch date of new one is bigger, replace old with new date
        if changed_rows.loc[index,"watched_date"]  > raw_stat.loc[index,"watched_date"]:
            raw_stat.loc[index,"watched_date"] = changed_rows.loc[index,"watched_date"]
        # only update neflix/prime status if status is not null (3)
        if changed_rows.loc[index, "netflix"] != -1:
            raw_stat.loc[index,"netflix"] = changed_rows.loc[index,"netflix"]
        if changed_rows.loc[index, "prime"] != -1:
            raw_stat.loc[index,"prime"] = changed_rows.loc[index,"prime"]
        # only update enjoyment is new value is not NA (-1)
        if changed_rows.loc[index, "enjoyment"] != -1:
            raw_stat.loc[index,"enjoyment"] = changed_rows.loc[index,"enjoyment"]
        # only update priority if old value is NA:
        if raw_stat.loc[index,"priority"] not in [-1, 1]:
            raw_stat.loc[index,"priority"] = changed_rows.loc[index,"priority"]

    # clean the dataframe
    raw_stat.loc[:,['netflix','prime','priority']] = raw_stat.loc[:,['netflix','prime','priority']].replace(-1,pd.NA)
    raw_stat[['enjoyment','story','subject','acting','visual','action','comedy']] = raw_stat[['enjoyment','story','subject','acting','visual','action','comedy']].replace(-1,np.NaN)
    raw_stat["watched_date"] = raw_stat["watched_date"].replace(pd.to_datetime("1900-1-1"), np.NaN)

    return raw_stat

main()