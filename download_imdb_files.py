# downloads all files and convert them to more logical parquet files.

# directors and writers are still not put in the right folder, and i don't know why!!!!
# I think it happens after title principals starts/crashes
# update: nothing is wrong before title.principals starts. 
#            the writers and directors parque files are in the right folder


import os
import re
import sys
import time
import gzip
import shutil
import requests
import numpy as np
import pandas as pd
from datetime import date
from datetime import timedelta

START_TIME = time.time()

DOWNLOAD = True
REMOVE_GZ_TSV = True

BASE_URL = "https://datasets.imdbws.com/"
PARQ_PATH = "data/imdb/parquet/"
DOWNLOAD_PATH = "data/imdb/download/"


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
    """ Downloads needed files and removes old files if they already exist. """
    END_TIME = time.time()
    TEMP_START_TIME = time.time()

    if not os.path.exists("data/imdb/download/"):
        os.makedirs("data/imdb/download")
    if not os.path.exists("data/imdb/parquet/"):
        os.makedirs("data/imdb/parquet")
    
    if REMOVE_GZ_TSV:
        # remove old files
        for file in FILES_IMDB_PARQ.values():
            file_name = os.path.join(PARQ_PATH,file)
            if os.path.exists(file_name):
                os.remove(file_name)
    
    # make dict with functions to execute
    funcs = {
        "cast_crew" : [ convertTitleCrew ],
        "tit_bas" : [ convertTitleBasics ],
        "tit_rate" : [ convertTitleRate ],
        "name_bas" : [ convertNameBasics, convertIds ],
        "tit_prin" : [ convertOrdering, convertCharacter, convertJob ],
        }

    #  download and prepare files
    for file_key, file_value in FILES_IMDB.items():
        TEMP_START_TIME = time.time()
        print("file: ",file_value)

        if DOWNLOAD:
            file_name = os.path.join(DOWNLOAD_PATH,file_value)
            file_zip = file_name+".gz"
            file_url = BASE_URL+file_value+".gz"

            # download file
            response = requests.get(file_url)
            open(file_zip , "wb").write(response.content)

            # unzip file
            with gzip.open(file_zip, 'rb') as f_in:
                with open(file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            if REMOVE_GZ_TSV:
                # remove zip
                os.remove(file_zip)

            END_TIME = time.time()
            part_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-TEMP_START_TIME))
            total_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
            print("\tdownload done. Ex. time: ",part_time, "\tTot. time: ",total_time)
            TEMP_START_TIME = time.time()

        # execute functions that are associated with the file
        for function in funcs[file_key]:
            function()
        
        if REMOVE_GZ_TSV:
            # files can be removed after downloading all files is done
            try:
                os.remove(os.path.join(DOWNLOAD_PATH,FILES_IMDB["tit_rate"]))
                os.remove(os.path.join(DOWNLOAD_PATH,FILES_IMDB["cast_crew"]))
            except:
                pass
        
        END_TIME = time.time()
        part_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-TEMP_START_TIME))
        total_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
        print("\tconversion done. Ex. time: ",part_time, "\tTot. time: ",total_time)
        TEMP_START_TIME = time.time()

    if REMOVE_GZ_TSV:
        # remove downloaded files
        try:
            os.remove(os.path.join(DOWNLOAD_PATH,FILES_IMDB["tit_bas"]))
            os.remove(os.path.join(DOWNLOAD_PATH,FILES_IMDB["tit_prin"]))
            os.remove(os.path.join(DOWNLOAD_PATH,FILES_IMDB["name_bas"]))
        except:
            pass


def setTNconst(df, key, set_index, drop_col):
    # helper function to convert id's to integers
    # tconst
    # We remove the first two tt and convert it to int.
    # Commands are not chained, baecause this can cause memory issues with big files.
    df.loc[:,key] = df.loc[:,key].str.slice(2)
    df.loc[:,key] = df.loc[:,key].astype('Int64')
    df.loc[:,key] = df.loc[:,key].astype('category')
    if set_index:
        df = df.set_index(key, drop=drop_col)
    return df


def convertIds():
    title_basics_path = os.path.join(DOWNLOAD_PATH,FILES_IMDB['tit_bas'])
    tconst = pd.read_csv(title_basics_path,sep='\t', quotechar='\t', low_memory=False, usecols=['tconst'])
    tconst = tconst.replace(to_replace = "\\N", value = np.nan)

    name_bas_path = os.path.join(DOWNLOAD_PATH,FILES_IMDB['name_bas'])
    nconst = pd.read_csv(name_bas_path,sep='\t', low_memory=True, usecols=['nconst'])
    nconst = nconst.replace(to_replace = "\\N", value = np.nan)

    # make new columns and rename
    tconst['stringid'] = tconst['tconst']
    tconst = tconst.rename(columns={'tconst': "intid"})
    tconst["type"] = 'tconst'

    nconst['stringid'] = nconst['nconst']
    nconst = nconst.rename(columns={'nconst': "intid"})
    nconst["type"] = 'nconst'

    # refactor id's
    const = setTNconst(pd.concat([tconst, nconst]), 'intid', set_index=False, drop_col=False)

    # set types
    const.loc[:,'stringid'] = const.loc[:,'stringid'].astype('category')
    const.loc[:,'type'] = const.loc[:,'type'].astype('category')
    const.loc[:,'intid'] = const.loc[:,'intid'].astype(int)

    # set index
    const = const.set_index("stringid", drop=True)

    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['const'])
    const.to_parquet(path, engine='pyarrow')
    
    # unnecessary, but i give it a try
    del const
    del tconst
    del nconst


def convertTitleBasics():
    # read
    title_basics_path = os.path.join(DOWNLOAD_PATH,FILES_IMDB['tit_bas'])
    title_basics = pd.read_csv(title_basics_path,sep='\t', quotechar='\t', low_memory=False)
    title_basics = title_basics.replace(to_replace = "\\N", value = np.nan)

    # convert and set tconst as index
    title_basics = setTNconst(title_basics, 'tconst', set_index=True, drop_col=True)

    # convert isAdult
    # Those movies are checked, and were not adult movies.
    # movies that are not an 1 or '1' aren't adult movies
    title_basics.loc[:,"isAdult"] = title_basics.loc[:,"isAdult"].fillna(0).astype('str')
    title_basics.loc[title_basics.loc[:,'isAdult'] != '1', 'isAdult'] = '0'
    title_basics.loc[:,"isAdult"] = title_basics.loc[:,"isAdult"].map({'1': True, '0': False})

    # auto convert
    title_basics = title_basics.convert_dtypes()

    # titleType
    # There are a few null values (np.nan)
    # convert to category
    title_basics.loc[:,"titleType"] = title_basics.loc[:,"titleType"].astype('category')

    # startYear & endYear & runtimeMinutes
    # both contain a lot of np.nan
    title_basics.loc[:,["startYear","endYear","runtimeMinutes"]] = title_basics.loc[:,["startYear","endYear","runtimeMinutes"]].astype('Int16')

    # make genres table
    genres = (title_basics
              .genres.str.split(",", expand=True)
              .stack()
              .reset_index()
              .rename(columns={"level_0":'tconst', 0:"genre"})
              .set_index("tconst")
              .drop(columns='level_1'))
    genres['genre'] = genres['genre'].astype('category')

    # drop genre
    title_basics = title_basics.drop(columns='genres')

    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['tit_bas'])
    title_basics.to_parquet(path, engine='pyarrow')
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['genres'])
    genres.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del title_basics
    del genres


def convertTitleRate():
    # read
    title_rate_path = os.path.join(DOWNLOAD_PATH,FILES_IMDB['tit_rate'])
    title_rate = pd.read_csv(title_rate_path,sep='\t', quotechar='\t', low_memory=False)
    title_rate = title_rate.replace(to_replace = "\\N", value = np.nan)

    # convert and set tconst as index
    title_rate = setTNconst(title_rate, 'tconst', set_index=True, drop_col=True)

    # convert averageRating
    title_rate.loc[:,'averageRating'] = title_rate.loc[:,'averageRating'].astype('float32')

    # convert numVotes
    title_rate.loc[:,'numVotes'] = title_rate.loc[:,'numVotes'].astype('uint32')

    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['tit_rate'])
    title_rate.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del title_rate


def convertTitleCrew():
    # read directors and writers file
    title_crew_path = os.path.join(DOWNLOAD_PATH,FILES_IMDB['cast_crew'])
    title_crew = pd.read_csv(title_crew_path,sep='\t', quotechar='\t', low_memory=True)
    title_crew = title_crew.replace(to_replace = "\\N", value = np.nan)

    # set index
    title_crew = setTNconst(title_crew, 'tconst', set_index=True, drop_col=True)

    for crew in ['directors', 'writers']:
        # make  dataframe
        crew_frame = (title_crew.loc[:,crew]
                    .dropna()
                    .str.split(",")
                    .to_frame()
                    .explode(crew))
        
        # convert
        crew_frame = setTNconst(crew_frame, crew, set_index=False, drop_col=False)
        crew_frame.loc[:,crew] = crew_frame.loc[:,crew].astype('category').to_frame()

        # write to disk
        path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ[crew])
        crew_frame.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del title_crew


def convertNameBasics():
    # load data
    name_bas_path = os.path.join(DOWNLOAD_PATH,FILES_IMDB['name_bas'])
    name_bas = pd.read_csv(name_bas_path,sep='\t', low_memory=True)
    name_bas = name_bas.replace(to_replace = "\\N", value = np.nan)

    # convert
    name_bas.loc[:,["nconst","primaryName","primaryProfession","knownForTitles"]] = name_bas.loc[:,["nconst","primaryName","primaryProfession","knownForTitles"]].astype('string')
    name_bas.loc[:,["birthYear","deathYear"]] = name_bas.loc[:,["birthYear","deathYear"]].astype('Int16')
    name_bas = setTNconst(name_bas, 'nconst', set_index=True, drop_col=True)
    name_convert = {'primaryProfession': 'prim_prof', 'knownForTitles': "known_for"}
    
    for info in ['primaryProfession', 'knownForTitles']:
        # get column and delete from original
        data = name_bas[info].dropna().to_frame()
        name_bas = name_bas.drop(columns=info)

        # explode data
        data = (data.loc[:,info]
             .str.split(',')
             .to_frame()
             .explode(info))
        
        # convert
        if info == 'primaryProfession':
            data.loc[:,info] = data.loc[:,info].astype('string').astype("category")
        else:
            data = setTNconst(data, info, set_index=False, drop_col=False)
        
        # write to disk
        path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ[name_convert[info]])
        data.to_parquet(path, engine='pyarrow')
    
    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['name_bas'])
    name_bas.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del name_bas


def convertOrdering():
    TEMP_START_TIME = time.time()
    # set vars
    chunk_size = int(55996061 / 3)
    tit_prin_path = os.path.join("data/imdb/download/", FILES_IMDB['tit_prin'])
    ordering = pd.DataFrame(columns=['tconst','nconst','ordering','category'])
    ordering = ordering.set_index('tconst')

    # load file using chunks
    for chunk in pd.read_csv(tit_prin_path,sep='\t', low_memory=True, usecols=['tconst', 'nconst', 'ordering', 'category'], chunksize=chunk_size):
        chunk = chunk.replace(to_replace = "\\N", value = np.nan)

        # set dtypes, convert id's and set index
        chunk.loc[:,'ordering'] = chunk.loc[:,'ordering'].astype(int)
        chunk = setTNconst(chunk, 'tconst', set_index=True, drop_col=True)
        chunk = setTNconst(chunk, 'nconst', set_index=False, drop_col=False)

        # merge chunks
        ordering = pd.concat([ordering, chunk]) 
    
    ordering.loc[:,'category'] = ordering.loc[:,'category'].astype(str).astype('category')

    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['ordering'])
    ordering.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del ordering

    END_TIME = time.time()
    part_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-TEMP_START_TIME))
    total_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("\tordering convert done. Ex. time: ",part_time, "\tTot. time: ",total_time)


def convertJob():
    TEMP_START_TIME = time.time()
    # set vars
    chunk_size = int(55996061 / 3)
    tit_prin_path = os.path.join("data/imdb/download/", FILES_IMDB['tit_prin'])
    job = pd.DataFrame(columns=['tconst', 'nconst', 'job'])
    job = job.set_index('tconst')

    
    # load file using chunks
    for chunk in pd.read_csv(tit_prin_path,sep='\t', low_memory=True, usecols=['tconst', 'nconst', 'job'], chunksize=chunk_size):
        chunk = chunk.replace(to_replace = "\\N", value = np.nan).dropna()

        # set dtypes, convert id's and set index
        chunk = setTNconst(chunk, 'tconst', set_index=True, drop_col=True)
        chunk = setTNconst(chunk, 'nconst', set_index=False, drop_col=False)
        chunk.loc[:,'job'] = chunk.loc[:,'job'].astype('category')

        # merge chunks
        job = pd.concat([job, chunk]) 

    # set job as category
    job.loc[:,'job'] = job.loc[:,'job'].astype(str).astype('category')

    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['job'])
    job.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del job

    END_TIME = time.time()
    part_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-TEMP_START_TIME))
    total_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("\tjob convert done. Ex. time: ",part_time, "\tTot. time: ",total_time)


def convertCharacter():
    TEMP_START_TIME = time.time()
    # set vars
    chunk_size = int(55996061 / 3)
    tit_prin_path = os.path.join("data/imdb/download/", FILES_IMDB['tit_prin'])
    character = pd.DataFrame(columns=['tconst', 'nconst', 'characters'])
    character = character.set_index('tconst')

    # load file using chunks
    for chunk in pd.read_csv(tit_prin_path,sep='\t', low_memory=True, usecols=['tconst', 'nconst', 'characters'], chunksize=chunk_size):
        chunk = chunk.replace(to_replace = "\\N", value = np.nan).dropna()

        # set dtypes, convert id's and set index
        chunk = setTNconst(chunk, 'tconst', set_index=True, drop_col=True)
        chunk = setTNconst(chunk, 'nconst', set_index=False, drop_col=False)

        # explode the list
        chunk.loc[:,"characters"] = chunk.loc[:,"characters"].apply(eval)
        chunk = chunk.explode('characters')

        # merge chunks
        character = pd.concat([character, chunk])

    # set characters as catagory
    chunk.loc[:,"characters"] = chunk.loc[:,"characters"].astype(str).astype('category')

    # write to disk
    path = os.path.join(PARQ_PATH, FILES_IMDB_PARQ['character'])
    character.to_parquet(path, engine='pyarrow')

    # unnecessary, but i give it a try
    del character

    END_TIME = time.time()
    part_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-TEMP_START_TIME))
    total_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("\tcharacter convert done. Ex. time: ",part_time, "\tTot. time: ",total_time)


main()