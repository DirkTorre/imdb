import warnings
import sys
from pathlib import Path

import pandas as pd



def get_status(file_path: Path) -> pd.DataFrame:
    """
    Loads and returns movie status data from the given file.

    Parameters
    ----------
    file_path : Path
        Path to the file that contains movie information regarding watched status and priority.

    Returns
    -------
    pd.DataFrame
        Contains info on the watch status. See the dtypes var below.
    """
    dtypes = {
        "tconst": pd.StringDtype(),
        "watched": pd.BooleanDtype(),
        "priority": pd.BooleanDtype(),
        "netflix": pd.BooleanDtype(),
        "prime": pd.BooleanDtype(),
    }
    status = pd.read_csv(file_path, dtype=dtypes, index_col="tconst")

    duplicated = status.index.duplicated()
    if duplicated.any():
        dups = str(list(status[duplicated].index))
        message = f"Duplicate indices found in:\n\t{file_path}\nPlease fix this: {dups}"
        warnings.warn(message, UserWarning)
        sys.exit(0)
    
    return status


def get_date_scores(file_path: Path) -> pd.DataFrame:
    """
    Loads and processes movie date and scores from the given file.

    Parameters
    ----------
    file_path : Path
        Path to the file that contains dates and scores of movies that are watched.

    Returns
    -------
    pd.DataFrame
        Contains dates movies are watched and (optional) movie review scores. See the dtypes var below.
    """
    dtypes = {
        "tconst": pd.StringDtype(),
        "enjoyment_score": pd.Float32Dtype(),
        "quality_score": pd.Float32Dtype(),
    }
    date_scores = pd.read_csv(
        file_path,
        dtype=dtypes,
        index_col="tconst",
        parse_dates=["date"],
    )

    # Convert datetime to date to retain only the date component
    date_scores["date"] = date_scores["date"].dt.date

    return date_scores
