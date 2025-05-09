import pandas as pd
from pathlib import Path


def write_excel(final_status: pd.DataFrame, final_date_scores: pd.DataFrame, file_path: Path) -> None:
    """
    Writes the final status and date scores data to an Excel file.

    Parameters
    ----------
    final_status : pd.DataFrame
        Contains movie information regarding watched status and priority.
    final_date_scores : pd.DataFrame
        Contains information regarding watch dates and scores.
    file_path : Path
        Destination path of the resulting excel file.
    """
    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        final_status.to_excel(writer, sheet_name="Status")
        final_date_scores.to_excel(writer, sheet_name="Date Scores")
