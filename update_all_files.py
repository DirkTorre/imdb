"""
- downloads fresh copy of the iMDb database (if var set)
- updates /data/handcrafted/raw_status.xslsx with movies in the two files:
    - /data/handcrafted/add_movies_seen.txt
    - - /data/handcrafted/add_movies_unseen.txt

todo: 
    - cleaningAndAddingSeenData() and cleaningAndAddingUnseenData() need to be made more efficient
    - prepareImdbFiles() must be made less redundant
    - tsv files must be removed (implemented. does it work?)
    - make parts of the clean...() scripts that is common into new functions
    - use the () for the linked methods to make code more readable
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

# set this var if you want to download a fresh copy of the latest iMDb movie data
DOWNLOAD = False
CONVERT = False
REMOVETSV = False
UPDATEWATCHLIST = False

def main():
    if DOWNLOAD:
        # 1 download the files
        downloadFiles()
    if CONVERT:
        # 2 prepare files for good small, fast storage
        prepareImdbFiles()
    
    if UPDATEWATCHLIST:
        # 3 update the watched list
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

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)


def downloadFiles():
    """ Downloads needed files and removes old files if already exist"""

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)
    print("start downloading")

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

        END_TIME = time.time()
        time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
        print("DOWNLOADED: "+file_name)
        print("Execution time: ",time_format)
    
    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)
    print("end downloading")


def prepareImdbFiles():
    # must be replace with a loop

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)

    print("converting title.ratings.tsv")
    # title.ratings.tsv
    tit_rate_path = os.path.join("data", "imdb", FILES_IMDB["tit_rate"])
    tit_rate = pd.read_csv(tit_rate_path, sep='\t', quotechar='\t', low_memory=False)
    tit_rate = cleanTitlesRate(tit_rate)
    tit_rate_tsv_path = os.path.join("data", "imdb", FILES_IMDB["tit_rate"])
    tit_rate.to_parquet(tit_rate_tsv_path.replace(".tsv",".parquet"))
    if REMOVETSV and os.path.exists(tit_rate_tsv_path):
        os.remove(tit_rate_tsv_path)
    del tit_rate

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)

    print("converting title.basics.tsv")
    # title.basics.tsv
    tit_bas_path = os.path.join("data", "imdb", FILES_IMDB["tit_bas"])
    tit_bas = pd.read_csv(tit_bas_path, sep='\t', quotechar='\t', low_memory=False)
    tit_bas = cleanTitlesBasic(tit_bas)
    tit_bas_tsv_path = os.path.join("data", "imdb", FILES_IMDB["tit_bas"])
    tit_bas.to_parquet(tit_bas_tsv_path.replace(".tsv",".parquet"))
    if REMOVETSV and os.path.exists(tit_bas_tsv_path):
        os.remove(tit_bas_tsv_path)
    del tit_bas

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)

    print("converting name.basics.tsv")
    # name.basics.tsv
    name_bas_path = os.path.join("data", "imdb", FILES_IMDB["name_bas"])
    name_bas = pd.read_csv(name_bas_path, sep='\t', low_memory=False)
    name_bas = cleanNameBasics(name_bas)
    name_bas_tsv_path = os.path.join("data", "imdb", FILES_IMDB["name_bas"])
    name_bas.to_parquet(name_bas_tsv_path.replace(".tsv",".parquet"))
    if REMOVETSV and os.path.exists(name_bas_tsv_path):
        os.remove(name_bas_tsv_path)
    del name_bas

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)

    print("converting title.crew.tsv")
    # title.crew.tsv
    cast_crew_path = os.path.join("data", "imdb", FILES_IMDB["cast_crew"])
    cast_crew = pd.read_csv(cast_crew_path, sep='\t', quotechar='\t', low_memory=False)
    cast_crew = CleanCrew(cast_crew)
    cast_crew_tsv_path = os.path.join("data", "imdb", FILES_IMDB["cast_crew"])
    cast_crew.to_parquet(cast_crew_tsv_path.replace(".tsv",".parquet"))
    if REMOVETSV and os.path.exists(cast_crew_tsv_path):
        os.remove(cast_crew_tsv_path)
    del cast_crew

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)

    print("converting title.principals.tsv")
    # title.principals.tsv
    tit_prin_path = os.path.join("data", "imdb", FILES_IMDB["tit_prin"])
    tit_prin = pd.read_csv(tit_prin_path, sep='\t', low_memory=True) # don't do quotechar with this one
    tit_prin = cleantitlePrinciples(tit_prin)
    tit_prin_tsv_path = os.path.join("data", "imdb", FILES_IMDB["tit_prin"])
    tit_prin.to_parquet(tit_prin_tsv_path.replace(".tsv",".parquet"))
    if REMOVETSV and os.path.exists(tit_prin_tsv_path):
        os.remove(tit_prin_tsv_path)
    del tit_prin

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)


def cleanTitlesBasic(tit_bas):
    tit_bas = tit_bas.replace(to_replace = "\\N", value = np.nan)
    
    # tconst
    # We remove the first two tt and convert it to int.
    tit_bas.loc[:,"tconst"] = tit_bas.loc[:,"tconst"].str.slice(2).astype(int)

    # titleType
    # There are a few null values (np.nan)
    # convert to category
    tit_bas.loc[:,"titleType"] = tit_bas.loc[:,"titleType"].astype('category')

    # primaryTitle & originalTitle
    # Is already either string or np.nan

    # isAdult
    # There are movies that have a year as adult status.
    # Those movies are checked, and were not adult movies.
    # movies that are not an 1 or '1' aren't adult movies
    tit_bas.loc[:,"isAdult"] = tit_bas.loc[:,"isAdult"].fillna(0).astype('str')
    tit_bas.loc[tit_bas.loc[:,'isAdult'] != '1', 'isAdult'] = '0'
    tit_bas.loc[:,"isAdult"] = tit_bas.loc[:,"isAdult"].map({'1': True, '0': False})

    # startYear & endYear & runtimeMinutes
    # both contain a lot of np.nan
    tit_bas.loc[:,"startYear"] = tit_bas.loc[:,"startYear"].astype('Int64')
    tit_bas.loc[:,"endYear"] = tit_bas.loc[:,"endYear"].astype('Int64')
    tit_bas.loc[:,"runtimeMinutes"] = tit_bas.loc[:,"runtimeMinutes"].astype('Int64')

    # genres
    # We don't convert the genres yet.
    # the way of transforming this attribute is dependent to the realtions of the elements in the list.
    # If the genres in the list have a correlation => 1 column for 1 genre.
    # If not: 1 new row for every genre for the movie
    # the quoted code converts the strings to a lists, then convert the values to boolean columns
    # yes, it's this stupendously easy
    # unique_values = list(set(",".join(tit_bas.loc[~tit_bas.loc[:,'genres'].isna(),'genres'].unique().tolist()).split(',')))
    # tit_bas.loc[:,'genres'] = tit_bas.loc[:,'genres'].str.split(',')
    # tit_bas.loc[:,'genres'] = tit_bas.loc[:,'genres'].fillna("")
    # new_columns = pd.DataFrame()
    # for value in unique_values:
    #     new_columns['genres_'+value] = tit_bas.loc[:,'genres'].apply(lambda x: value in x)
    # tit_bas = tit_bas.drop('genres', axis=1)
    # tit_bas = pd.concat([tit_bas, new_columns], axis=1)
    tit_bas.loc[:,'genres'] = tit_bas.loc[:,'genres'].str.split(',')

    return tit_bas


def cleanTitlesRate(tit_rate):
    tit_rate = tit_rate.replace(to_replace = "\\N", value = np.nan)
    
    # tconst
    # We remove the first two tt and convert it to int.
    tit_rate.loc[:,"tconst"] = tit_rate.loc[:,"tconst"].str.slice(2).astype(int)

    # averageRating & numVotes
    # There are no missing values, we only have to convert them.
    tit_rate.loc[:,'averageRating'] = tit_rate.loc[:,'averageRating'].astype(float)
    tit_rate.loc[:,'numVotes'] = tit_rate.loc[:,'numVotes'].astype(int)

    return tit_rate

def cleanNameBasics(name_bas):
    name_bas = name_bas.replace(to_replace = "\\N", value = np.nan)
    
    # nconst
    # We remove the first two nm and convert it to int.
    name_bas.loc[:,"nconst"] = name_bas.loc[:,"nconst"].str.slice(2).astype(int)

    # primaryName
    # does not need modification
    # has 3 missing values
    # name_bas.loc[name_bas.loc[:,"primaryName"].isna(),:]

    # birthYear & deathYear has a lot of missing values
    # some are year 4, or 12. those are greek writers like nm0653992 (Ovid) 
    # which play was used for the movie.
    name_bas.loc[:,"birthYear"] = name_bas.loc[:,"birthYear"].astype('Int64')
    name_bas.loc[:,"deathYear"] = name_bas.loc[:,"deathYear"].astype('Int64')

    # primaryProfession
    # There are 43 unique values.
    # contains NaN values
    # If the values are independent form each other, then we must transform them to 1 row/value.
    # If they are dependent on each other, then we must transform then as 1 column/value.
    name_bas.loc[:,'primaryProfession'] = name_bas.loc[:,'primaryProfession'].str.split(',')

    # knownForTitles
    # interesting info, but probably better to remove it.
    name_bas.loc[:,'knownForTitles'] = name_bas.loc[:,'knownForTitles'].str.replace("tt","").str.split(',')
    tempknowntit = name_bas.loc[~name_bas.loc[:,'knownForTitles'].isna(),['nconst','knownForTitles']].explode("knownForTitles").astype("Int64").copy()
    tempknowntit = tempknowntit.groupby('nconst')['knownForTitles'].apply(list)
    name_bas = name_bas.drop(columns='knownForTitles')
    name_bas = pd.merge(name_bas, tempknowntit, how='left', on="nconst")

    return name_bas


def CleanCrew(cast_crew):
    cast_crew = cast_crew.replace(to_replace = "\\N", value = np.nan)

    # tconst
    # We remove the first two tt and convert it to int.
    cast_crew.loc[:,"tconst"] = cast_crew.loc[:,"tconst"].str.slice(2).astype(int)

    # directors
    # there are NO missing values
    cast_crew.loc[:,"directors"] = cast_crew.loc[:,"directors"].str.replace("nm","").str.split(',')
    tempcastcrew = cast_crew.loc[~cast_crew.loc[:,'directors'].isna(),['tconst','directors']].explode("directors").astype("Int64").copy()
    tempcastcrew = tempcastcrew.groupby('tconst')['directors'].apply(list)
    cast_crew = cast_crew.drop(columns='directors')
    cast_crew = pd.merge(cast_crew, tempcastcrew, how='left', on="tconst")

    # writers
    # there ARE missing values
    cast_crew.loc[:,"writers"] = cast_crew.loc[:,"writers"].str.replace("nm","").str.split(',')
    tempcastcrew = cast_crew.loc[~cast_crew.loc[:,'writers'].isna(),['tconst','writers']].explode("writers").astype("Int64").copy()
    tempcastcrew = tempcastcrew.groupby('tconst')['writers'].apply(list)
    cast_crew = cast_crew.drop(columns='writers')
    cast_crew = pd.merge(cast_crew, tempcastcrew, how='left', on="tconst")

    return cast_crew


def cleantitlePrinciples(tit_prin):
    tit_prin = tit_prin.replace(to_replace = "\\N", value = np.nan)

    # tconst
    # We remove the first two tt and convert it to int.
    # File is to big to do it in one go.
    tit_prin.loc[:,"tconst"] = tit_prin.loc[:,"tconst"].str.slice(2)
    tit_prin.loc[:,"tconst"] = tit_prin.loc[:,"tconst"].astype('Int64')

    # ordering
    # data is clean
    tit_prin.loc[:,"ordering"] = tit_prin.loc[:,"ordering"].astype('Int64')

    # nconst
    # We remove the first two tt and convert it to int.
    # all values start with nm
    tit_prin.loc[:,"nconst"] = tit_prin.loc[:,"nconst"].str.slice(2)
    tit_prin.loc[:,"nconst"] = tit_prin.loc[:,"nconst"].astype('Int64')

    # category
    # data is clean
    tit_prin.loc[:,"category"] = tit_prin.loc[:,"category"].astype('category')

    # job
    # There are a lot of values that are the same:
    # 'writer' and 'written by'; 'creator' and 'created by' etc 
    # tit_prin.loc[:,"job"].value_counts()

    # characters
    # there are a lot of missing values
    
    return tit_prin


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
                'tconst': movieid, 'watched':1, 'netflix':np.nan,
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