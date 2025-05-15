from pathlib import Path
from datetime import datetime
import argparse
import os

import src.imdb.download as download
import src.imdb.load as load
import src.read_write.csv as read_write_csv
import src.read_write.excel as read_write_excel
import src.visualization.recommendations as rec

from bokeh.models import (ColumnDataSource, HoverTool, DataTable, TableColumn, DataCube, GroupingInfo, GroupFilter, CDSView, TabPanel, Tabs, Div)
from bokeh.plotting import figure, show
from bokeh.layouts import column
import pandas as pd

def main() -> None:
    """Main function to process and generate the watch list."""

    parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-u", "--update", action="store_true", help="Downloads a fresh IMDb dataset")
    parser.add_argument("-d", "--dashboard", action="store_true", help="Creates a movie recommendations dashboard")
    parser.add_argument("-e", "--excel", action="store_true", help="create excel")
    parser.add_argument("-r", "--reuse", action="store_true", help="Reuse previously generated data")
    args = vars(parser.parse_args())

    # Define file paths
    base_path = Path(__file__).parent.resolve()
    imdb_files_path = base_path / "data" / "downloads"
    sheets_path = base_path / "data" / "sheets"
    status_path_temp = base_path / "data" / "status.pickle"

    # Check if there are no imdb files on disk
    no_tit_bas = not os.path.exists(imdb_files_path / "title.basics.tsv.gz")
    no_tit_rat = not os.path.exists(imdb_files_path / "title.ratings.tsv.gz")
    imdb_files_not_avaialble = no_tit_bas or no_tit_rat

    # Always update files when prompted or when there are no files
    if args["update"] or imdb_files_not_avaialble:
        # Download IMDb files
        files_to_download = {"title.basics": True, "title.ratings": True}
        download.Download(imdb_files_path, files_to_download).download_files()
        print("Files downloaded")

    if args["excel"] or not args["reuse"]:
        print("Assembling data")
        # Import movie status data
        date_scores_path = sheets_path / "date_scores.csv"
        status_path = sheets_path / "status.csv"

        date_scores = read_write_csv.get_date_scores(date_scores_path)
        status = read_write_csv.get_status(status_path)

        # Retrieve movie info
        needed_indices = date_scores.index.union(status.index)
        title_basics_path = imdb_files_path / "title.basics.tsv.gz"
        title_ratings_path = imdb_files_path / "title.ratings.tsv.gz"

        title_basics = load.get_title_basic(needed_indices, title_basics_path)
        title_ratings = load.get_title_rating(needed_indices, title_ratings_path)

        # Prepare final status data
        final_status = (
            status.join(title_ratings)
            .join(title_basics)
            .sort_values(
                ["watched", "priority", "averageRating"], ascending=[True, False, False]
            )
        )

        final_status.to_pickle(status_path_temp)

    if args["reuse"]:
        print("Reusing data")
        final_status = pd.read_pickle(status_path_temp)

    if args["excel"]:
        # Prepare final date scores data
        final_date_scores = date_scores.join(
            title_basics[["primaryTitle", "originalTitle", "startYear"]]
        ).sort_values("date")

        # Write data to an Excel file
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        watch_list_path = sheets_path / f"watch_list_{timestamp}.xlsx"
        read_write_excel.write_excel(final_status, final_date_scores, watch_list_path)
        print(f"Created excel file can be found at: {watch_list_path}")
    
    # create the data recommendation visualization
    if args["dashboard"]:
        print("Creating movie recommendation visualization")
        rec.create_movie_recommendations(final_status)


if __name__ == "__main__":
    main()
