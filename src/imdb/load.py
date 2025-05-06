from typing import List
import pandas as pd


def getTitleBasic(tconst_index, file_path: str):
    cols_to_use = ['tconst', 'primaryTitle',  'originalTitle', 'startYear', 'runtimeMinutes', 'genres']
    dtypes = {'startYear': pd.Int32Dtype(), "runtimeMinutes": pd.Int32Dtype()}
    
    title_basics = pd.read_csv(
        file_path, sep='\t', quotechar='\t', low_memory=False, 
        dtype_backend="pyarrow", usecols=cols_to_use, index_col=["tconst"], dtype=dtypes, na_values="\\N")
    
    title_basics = title_basics.loc[tconst_index]
    
    genre_exploded = title_basics["genres"].str.split(',').explode()
    genre = pd.crosstab(genre_exploded.index, genre_exploded)
    genre.columns = genre.columns.values.tolist()
    genre = genre.add_prefix("genre_")
    genre.index.names=["tconst"]
    genre.astype(pd.BooleanDtype())
    genre = genre.astype(pd.BooleanDtype())

    return title_basics.drop(columns="genres").join(genre)


def getTitleRating(tconst_index, file_path: str):
    dtypes = {'averageRating': pd.Float32Dtype(), "numVotes": pd.Int32Dtype()}

    title_ratings = pd.read_csv(
        file_path, sep='\t', quotechar='\t', low_memory=False, 
        dtype_backend="pyarrow", index_col=["tconst"], dtype=dtypes, na_values="\\N")
    
    return title_ratings.loc[tconst_index]

