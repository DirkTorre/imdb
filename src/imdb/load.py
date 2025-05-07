from typing import List
import pandas as pd


def get_title_basic(tconst_index: List[str], file_path: str) -> pd.DataFrame:
    """Fetches title basic information from the given file."""
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

    # Select specific tconst entries
    title_basics = title_basics.loc[tconst_index]

    # Process genres
    genre_exploded = title_basics["genres"].str.split(",").explode()
    genre = pd.crosstab(genre_exploded.index, genre_exploded)
    genre = genre.add_prefix("genre_").astype(pd.BooleanDtype())

    return title_basics.drop(columns="genres").join(genre)


def get_title_rating(tconst_index: List[str], file_path: str) -> pd.DataFrame:
    """Fetches title ratings from the given file."""
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

    return title_ratings.loc[tconst_index]
