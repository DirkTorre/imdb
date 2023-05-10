"""
Takes the movie list and appends IMDb data to it.
Made as a class for use with other programs by inheritance.
Output is an ordered excel file.
"""
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
    """The method takes in a lot of data paths."""
    id_stat =       os.path.join("data", "handcrafted", FILES_HAND["raw_status"])
    const =         os.path.join(PARQ_PATH, FILES_IMDB_PARQ["const"])
    tit_bas =       os.path.join(PARQ_PATH, FILES_IMDB_PARQ["tit_bas"])
    tit_rat =       os.path.join(PARQ_PATH, FILES_IMDB_PARQ["tit_rate"])
    genre =         os.path.join(PARQ_PATH, FILES_IMDB_PARQ["genres"])
    directors =     os.path.join(PARQ_PATH, FILES_IMDB_PARQ["directors"])
    writers =       os.path.join(PARQ_PATH, FILES_IMDB_PARQ["writers"])
    personnel =     os.path.join(PARQ_PATH, FILES_IMDB_PARQ["ordering"])
    name_basics =   os.path.join(PARQ_PATH, FILES_IMDB_PARQ["name_bas"])
    output_excel =   os.path.join(OUTPUT_PATH, FILES_GENERATED["films_reading"])
    # output_excel =   os.path.join(OUTPUT_PATH, "test.xlsx")

    # Make a instance
    test = AppendedMovieList(
        url_movie_list = id_stat,
        url_imdb_ids = const,
        url_title_basics = tit_bas,
        url_title_rate = tit_rat,
        url_genre = genre,
        url_directors = directors,
        url_writers = writers,
        url_personnel =  personnel,
        url_name_basics = name_basics
        )

    # Get created movie list and write to excel.
    movies = test.getMovieList()
    movies.to_excel(output_excel, index=False)

    END_TIME = time.time()
    time_format = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))
    print("Execution time: ",time_format)



class AppendedMovieList():
    BASE_URL = "https://datasets.imdbws.com/"
    PARQ_PATH = "data/imdb/parquet/"
    DOWNLOAD_PATH = "data/imdb/download/"

    def __init__(self,
                 url_movie_list,
                 url_imdb_ids,
                 url_title_basics,
                 url_title_rate,
                 url_genre,
                 url_directors,
                 url_writers,
                 url_personnel,
                 url_name_basics
                 ):
        
        self.__movie_list = pd.DataFrame()
        self.url_movie_list = url_movie_list
        self.url_imdb_ids = url_imdb_ids
        self.url_title_basics = url_title_basics
        self.url_title_rate = url_title_rate
        self.url_genre = url_genre
        self.url_directors = url_directors
        self.url_writers = url_writers
        self.url_personnel = url_personnel
        self.url_name_basics = url_name_basics



        self._loadList()
        self._addMovieIds()
        self._addTitleBasics()
        self._addRating()
        self._addGenre()
        self._addPersonnel()


    def _loadList(self):
        # load the raw movie list
        self.__movie_list = pd.read_excel(self.url_movie_list)

        # convert date
        self.__movie_list['watched_date'] = self.__movie_list['watched_date'].dt.date


    def _addMovieIds(self):
        # load movieID's
        const = pd.read_parquet(self.url_imdb_ids)
        
        # add movieID's
        self.__movie_list = pd.merge(
            self.__movie_list, const[const["type"]=="tconst"]["intid"],
            how='left', left_on="tconst", right_index=True)
        
        # set index
        self.__movie_list = self.__movie_list.set_index('intid', drop=True)
    

    def _addTitleBasics(self):
        # load title basics
        tit_bas = pd.read_parquet(self.url_title_basics)

        # delete adult column
        tit_bas = tit_bas.drop(columns='isAdult')

        # add title basics
        self.__movie_list = pd.merge(self.__movie_list, tit_bas,
                                     how='left', left_index=True, right_index=True)
    

    def _addRating(self):
        # load ratings
        tit_rate = pd.read_parquet(self.url_title_rate)

        # add title rate
        self.__movie_list = pd.merge(self.__movie_list, tit_rate, how='left',
                                  left_index=True, right_index=True)
        
        # convert rate
        self.__movie_list["averageRating"] = self.__movie_list["averageRating"].astype('float64')
        self.__movie_list["numVotes"] = self.__movie_list["numVotes"].astype('Int64')


    def _addGenre(self):
        # load genres
        genre = pd.read_parquet(self.url_genre)

        # get needed ones
        genre = genre[genre.index.isin(self.__movie_list.index)]
        genre['genre'] = genre['genre'].astype('category')

        # convert to multi one-hot
        genre = pd.crosstab(genre.index, genre['genre'])
        
        # add genre
        self.__movie_list = pd.merge(self.__movie_list, genre, how='left', left_index=True, right_index=True)


    def _loadPersonnel(self):
        # load neaded directors
        directors = pd.read_parquet(self.url_directors)
        directors = directors[directors.index.isin(self.__movie_list.index)]
        directors['category'] = 'director'
        directors = directors.rename(columns={'directors':'nconst'})

        # load needed writers
        writers = pd.read_parquet(self.url_writers)
        writers = writers[writers.index.isin(self.__movie_list.index)]
        writers['category'] = 'writer'
        writers = writers.rename(columns={'writers':'nconst'})

        # load needed ordering
        personnel = pd.read_parquet(self.url_personnel)
        personnel = personnel[personnel.index.isin(self.__movie_list.index)].drop(columns='ordering')

        # combine personell id's
        all_personnel = pd.concat([writers, directors, personnel])
        all_personnel = all_personnel.drop_duplicates()
        all_personnel.loc[:,'category'] = all_personnel.loc[:,'category'].astype('category')

        return all_personnel
    
    
    def _addPersonnel(self):
        # prepare personell ids
        all_personnel = self._loadPersonnel()

        # get needed personell info
        name_bas = pd.read_parquet(self.url_name_basics)
        names = name_bas[name_bas.index.isin(all_personnel['nconst'])]

        # convert personell info
        names = names["primaryName"] + " (" + \
            names["birthYear"].astype(str).replace("<NA>","") + \
                "-" + names["deathYear"].astype(str).replace("<NA>","") + ")"
        names.name = "info"

        # add personell info to movie indices
        personell = pd.merge(all_personnel, names, how='left', left_on='nconst', right_index=True).drop(columns='nconst')

        # convert personell categories to columns
        personell = pd.pivot_table(personell, values='info', index=['tconst'], columns=['category'], aggfunc=list)

        # add personell to watched movies (can probably be replaced by a normal add)
        self.__movie_list = pd.merge(self.__movie_list, personell, how='left', left_index=True, right_index=True)


    def getMovieList(self):
        # probably better to group and sort the groups, but whatever.
        # Order first by watched date; 
        # then order the wachted movies without date on score; 
        # then put unwatched movies at the bottom with highest score on top.
        watched = self.__movie_list.query('watched==1')
        watched_no_date = watched[watched['watched_date'].isna()].sort_values('averageRating', ascending=False)
        watched_date = watched[~watched['watched_date'].isna()].sort_values('watched_date', ascending=False)
        not_watched = self.__movie_list.query('watched==0').sort_values('averageRating', ascending=False)
        
        return pd.concat([not_watched, watched_date, watched_no_date])


main()
