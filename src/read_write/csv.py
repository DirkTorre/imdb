import pandas as pd
from pathlib import Path
from datetime import datetime

def getStatus(file_path: Path):
    dtypes = {
        "tconst": pd.StringDtype(),
        "watched": pd.BooleanDtype(),
        "priority": pd.BooleanDtype(),
        "netflix": pd.BooleanDtype(),
        "prime": pd.BooleanDtype()}
    return pd.read_csv(file_path, dtype=dtypes,index_col="tconst")


def getDateScores(file_path: Path):
    dtypes = {
        "tconst": pd.StringDtype(),
        "enjoyment_score": pd.Float32Dtype(),
        "quality_score": pd.Float32Dtype()
        }
    date_scores = pd.read_csv(file_path, dtype=dtypes, index_col="tconst", parse_dates=['date'], date_format="%Y-%m-%d")
    date_scores['date'] = pd.to_datetime(date_scores['date']).dt.date
    return date_scores