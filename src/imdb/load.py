from typing import List
import pandas as pd
from pathlib import Path


def get_title_basic(tconst_index: List[str], file_path: Path) -> pd.DataFrame:
    """
    Fetches title basic information from the given file.

    Parameters
    ----------
    tconst_index : List[str]
        List of movie identifiers (tconst column). string: tt<number>
    file_path : Path
        Full path of the file title.basics.tsv.gz. No need to unzip.

    Returns
    -------
    pd.DataFrame
        Subset of title.basics.tsv where genres are turned into columns.
    """
    cols_to_use = [
        "tconst",
        "primaryTitle",
        "originalTitle",
        "startYear",
        "runtimeMinutes",
        "genres",
    ]
    dtypes = {"startYear": pd.Int32Dtype(), "runtimeMinutes": pd.Int32Dtype()}

    title_basics = pd.read_csv(
        file_path,
        sep="\t",
        quotechar="\t",
        low_memory=False,
        dtype_backend="pyarrow",
        usecols=cols_to_use,
        index_col="tconst",
        dtype=dtypes,
        na_values="\\N",
    )

    # Get missing indices
    not_in_title_basics = tconst_index.difference(title_basics.index)

    # Get subset with existing indices
    title_basics = title_basics.loc[tconst_index.intersection(title_basics.index)]

    # Process genres
    genre_exploded = title_basics["genres"].str.split(",").explode()
    genre = pd.crosstab(genre_exploded.index, genre_exploded)
    genre = genre.add_prefix("genre_").astype(pd.BooleanDtype())

    return title_basics.drop(columns="genres").join(genre), not_in_title_basics


def get_title_rating(tconst_index: List[str], file_path: str) -> pd.DataFrame:
    """
    Fetches title ratings from the given file.

    Parameters
    ----------
    tconst_index : List[str]
        List of movie identifiers (tconst column). string: tt<number>
    file_path : str
        Full path of the file title.ratings.tsv.gz. No need to unzip.

    Returns
    -------
    pd.DataFrame
        Subset of title.ratings.tsv.
    """
    dtypes = {"averageRating": pd.Float32Dtype(), "numVotes": pd.Int32Dtype()}

    title_ratings = pd.read_csv(
        file_path,
        sep="\t",
        quotechar="\t",
        low_memory=False,
        dtype_backend="pyarrow",
        index_col="tconst",
        dtype=dtypes,
        na_values="\\N",
    )

    # Get missing indices
    not_in_title_ratings = tconst_index.difference(title_ratings.index)

    # Get subset with existing indices
    title_ratings = title_ratings.loc[tconst_index.intersection(title_ratings.index)]

    return title_ratings, not_in_title_ratings
