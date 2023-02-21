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
    "films_raw": "films_raw.pkl"
}

# set this var if you want to downoad a fresh copy of the latest iMDb movie data
DOWNLOAD = False


def main():
    ## load the core files
    ######################

    # 3 load and clean the watched movies data
    ids_and_status = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    watched = pd.read_excel(ids_and_status)
    watched = loadAndCleanWatchedData(watched)

    # 4 load the files that are needed to extend the watched movies
    title_basics_path = os.path.join("data", "imdb", FILES_IMDB["tit_bas"])
    title_basics = pd.read_csv(title_basics_path, sep="\t")
    title_rate_path = os.path.join("data", "imdb", FILES_IMDB["tit_rate"])
    title_rate = pd.read_csv(title_rate_path, sep="\t")
    cast_crew_mega_path = os.path.join("data", "imdb", FILES_IMDB["cast_crew"])

    # 5 generate the dataset
    watched = addIMDbData(watched, title_basics, title_rate, cast_crew_mega_path)
    saveAsPickle(watched)


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
            
            seen_raw[linei] = [ttcode, row2, filmdate]


    ## getting the new unseen movies
    ################################

    # transforming the new unseen movie data
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

    ## Adding the 2d list to the raw_watched.xlsx (if not already added)
    ####################################################################

    ids_and_status = os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    movie_list_raw = pd.read_excel(ids_and_status)

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
            # watched_date = movie_list_raw.at[found_index,"watched_date"]
            if watched==1:
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
                'prime':np.nan, "enjoyment":score , "priority": np.nan})

            movie_list_raw = pd.concat([movie_list_raw, to_add.to_frame().T], ignore_index=True)
    
    for movieid in unseen_raw:
        if not movieid in movie_list_raw["tconst"].values:
            # if watched movie not already in list
            to_add = pd.Series({
                'tconst':movieid, 'watched':0, 'netflix':np.nan,
                'prime':np.nan, "enjoyment":np.nan , "priority": np.nan})

            movie_list_raw = pd.concat([movie_list_raw, to_add.to_frame().T], ignore_index=True)
    
    return movie_list_raw


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
    # title_watched = pd.merge(watched, title_basics, on="tconst", how="left") # new merge, keeps wrong stuff
    watched_title = pd.merge(watched, title_basics, on="tconst", how="left") # new merge, keeps wrong stuff
    del title_basics # cleanup memory by force
    return watched_title


def addRatings(watched_title, title_rate):
    title_rate = title_rate.replace(to_replace = "\\N", value = np.nan)
    title_rate.loc[:,"numVotes"] = title_rate.loc[:,"numVotes"].astype('Int64')
    # watched_film_fin = pd.merge(watched_title,title_rate, on="tconst", how="left")
    watched_title_rate = pd.merge(watched_title,title_rate, on="tconst", how="left")
    del watched_title # cleanup memory by force
    return watched_title_rate


def addCastAndCrew(watched_title_rate, cast_crew_mega_file):
    # get cast and crew from the big file
    #####################################
    watched_films_cast  = pd.DataFrame(columns=["tconst", "ordering","nconst", "category", "job", "characters"])

    for chunk in pd.read_csv(cast_crew_mega_file, sep="\t", chunksize=1000):
        rows = pd.merge(watched_title_rate.loc[:,"tconst"], chunk, on="tconst", how="inner")
        watched_films_cast = pd.concat([rows,watched_films_cast], ignore_index = True)

    watched_films_cast.replace(to_replace = "\\N", value = np.nan, inplace=True)
    watched_films_cast.drop(['characters'], axis=1, inplace=True)
    
    # add cast and crew
    ###################
    watched_title_rate_personel = pd.merge(watched_title_rate, watched_films_cast, how='inner', on='tconst')
    del watched_title_rate
    del watched_films_cast

    # add info about personell
    ##########################
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


def saveAsPickle(watched):
        output = os.path.join("data", "generated", FILES_GENERATED["films_raw"])
        watched.to_pickle(output)

        END_TIME = time.time()
        time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
        print("Execution time: ",time_format)


if __name__ == "__main__":
    main()