from pathlib import Path
from datetime import datetime
import pandas as pd

import src.imdb.download as download
import src.imdb.load as load
import src.read_write.csv as read_write_csv
import src.read_write.excel as read_write_excel




def main():

    # Download the imdb files
    imdb_files_path = Path(__file__).parent.resolve() / "data/downloads"
    files_to_download = {"title.basics": False, "title.ratings": True}
    download_obj = download.Download(files_path=imdb_files_path, files_to_download=files_to_download)
    download_obj.download_files()

    # Import the movie status data
    sheets_path = Path(__file__).parent.resolve() / "data" / "sheets"
    date_scores_path = sheets_path / "date_scores.csv"
    status_path = sheets_path / "status.csv"

    date_scores = read_write_csv.getDateScores(file_path=date_scores_path)
    status = read_write_csv.getStatus(file_path=status_path)

    # Retrieve movie info
    needed_indices = date_scores.index.append(status.index).unique()
    downloads_path = Path(__file__).parent.resolve() / "data" / "downloads"
    title_basics_path = downloads_path / "title.basics.tsv.gz"
    title_ratings_path = downloads_path / "title.ratings.tsv.gz"

    # get movie info for the given movie id's.
    title_basics = load.getTitleBasic(needed_indices, title_basics_path)
    
    
    # get the scores for the movie id's
    title_ratings = load.getTitleRating(needed_indices, title_ratings_path)
    

    final_status = status.join(title_ratings).join(title_basics).sort_values(["watched", "priority","averageRating"], ascending=[True,False,False])
    # title_ratings already contains all info, so we can use this
    final_date_scores = date_scores.join(title_basics[['primaryTitle',  'originalTitle', 'startYear']]).sort_values("date")

    
    # Write the data to an excel file
    time_stamp = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
    watch_list_path = sheets_path / f"watch_list_{time_stamp}.xlsx"
    read_write_excel.writeExcel(final_status, final_date_scores, watch_list_path)
    
    

if __name__ == "__main__":
    main()