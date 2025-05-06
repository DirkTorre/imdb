from datetime import datetime
import pandas as pd
from pathlib import Path


def writeExcel(final_status: pd.DataFrame, final_date_scores: pd.DataFrame, file_path: Path):
    with pd.ExcelWriter(file_path) as writer:
        final_status.to_excel(writer, sheet_name="status")
        final_date_scores.to_excel(writer, sheet_name="date_scores")
