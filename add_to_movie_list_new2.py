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
    "raw_status": "raw_status.xlsx",
    "to_add" : "to_add.xlsx"
}

FILES_GENERATED = {
    "films_raw": "films_raw.pkl",
    "films_reading": "films_reading.xlsx"
}


def setAttr(frame):
    # setting column types
    frame['watched_date'] = pd.to_datetime(frame['watched_date'])
    frame[['enjoyment','story','subject','acting','visual','action','comedy']] = frame[['enjoyment','story','subject','acting','visual','action','comedy']].astype(float)
    frame['watched'] = frame['watched'].astype("Int64").replace(0,np.nan)
    frame[['netflix','prime','priority']] = frame[['netflix','prime','priority']].astype("Int64")
    return frame


def loadData():
    """Loads the raw excel files."""
    # load data and set types of films to add
    id_stat = os.path.join("data", "handcrafted", FILES_HAND["to_add"])
    to_add = setAttr(pd.read_excel(id_stat))
    # convert link to tconstant
    to_add['link'] = to_add['link'].str.split("/",expand=True).loc[:,4].astype(str)
    # remove duplicates
    to_add = to_add.drop_duplicates().rename(columns={"link":"tconst"})
    # add index as column
    to_add["row_index"] = to_add.index
    to_add = to_add.set_index("tconst")
    # set nan to 0
    to_add.loc[:,['priority', 'watched']] = to_add.loc[:,['priority', 'watched']].fillna(0)

    # loading and preparing film list
    raw_stat_link = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    raw_stat = setAttr(pd.read_excel(raw_stat_link))
    raw_stat = raw_stat.drop_duplicates().set_index("tconst")
    # set nan to 0
    raw_stat.loc[:,['priority', 'watched']] = raw_stat.loc[:,['priority', 'watched']].fillna(0)

    return to_add, raw_stat


def removeDuplicates(to_add):
    # the new and improved function 26 jan 2024
    # fixing watched, priority, netflix, prime
    # replace value with highest value (if a movie has a 1, all duplicates get to be 1)

    # Because new data is appended at the end (CHECK THIS),
    # we can identify new data by a bigger row number.
    if "row_index" not in to_add.columns:
        to_add["row_index"] = range(to_add.shape[0])

    for col_name in ["watched", "priority", "netflix", "prime"]:
        new = (to_add
            .sort_values(["tconst", col_name],ascending=False)
            .reset_index()
            .drop_duplicates(subset=["tconst"],keep="first")
            .loc[:,["tconst", col_name]])
        to_add.update(new.set_index("tconst"), overwrite=True)

    # fixing scores
    # keep the latest score, unless the is no score than move to the next
    for score_cat in ["enjoyment", "story", "subject", "acting", "visual", "action", "comedy"]:
        new_score = (to_add.sort_values(["tconst","watched_date","row_index"],ascending=False)
                     .reset_index()
                     .loc[:,["tconst", score_cat]]
                     .dropna()
                     .drop_duplicates(subset=["tconst"],keep="first"))
        to_add.update(new_score.set_index("tconst"), overwrite=True)

    # effective way of assigning the latest date to a movie.
    # keep in mind that this must be done at the end, otherwise is screws over the ordering.
    new_watched_date_values = (to_add
                               .reset_index()
                               .sort_values(["tconst","watched_date"],ascending=False)
                               .drop_duplicates(subset=["tconst"],keep="first")[["tconst","watched_date"]]
                               .set_index("tconst"))
    to_add.update(new_watched_date_values)

    # drop row_index column
    to_add = to_add.drop(columns="row_index")

    # remove rows that have no data
    to_add = to_add[to_add.index!="nan"]

    # removing duplicates
    return to_add.reset_index().drop_duplicates(ignore_index=False).set_index("tconst")


def main():
    to_add, raw_stat = loadData()
    to_add = removeDuplicates(to_add)

    # Merge the old data with the new data.
    # Keep in mind that duplicates that were in in raw_stat from the start are also removed.
    # It's important to add the newest data at the end.
    # This is because we are going to number the rows.
    # Older data get's a higher number.
    raw_stat = removeDuplicates(pd.concat([raw_stat, to_add]))

    # overwrite raw_status
    output = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    raw_stat.sort_index().to_excel(output)

    # empty to_add.xlsx
    new_empty = pd.DataFrame(data=None, columns=["link"]+to_add.columns.to_list())
    to_add = os.path.join("data", "handcrafted", FILES_HAND["to_add"])
    new_empty.to_excel(to_add, index=False)


main()