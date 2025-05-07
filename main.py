from pathlib import Path
from datetime import datetime

import src.imdb.download as download
import src.imdb.load as load
import src.read_write.csv as read_write_csv
import src.read_write.excel as read_write_excel


def main() -> None:
    """Main function to process and generate the watch list."""
    
    # Define file paths
    base_path = Path(__file__).parent.resolve()
    imdb_files_path = base_path / "data" / "downloads"
    sheets_path = base_path / "data" / "sheets"

    # Download IMDb files
    files_to_download = {"title.basics": False, "title.ratings": True}
    download.Download(imdb_files_path, files_to_download).download_files()

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
        .sort_values(["watched", "priority", "averageRating"], ascending=[True, False, False])
    )

    # Prepare final date scores data
    final_date_scores = (
        date_scores.join(title_basics[["primaryTitle", "originalTitle", "startYear"]])
        .sort_values("date")
    )

    # Write data to an Excel file
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    watch_list_path = sheets_path / f"watch_list_{timestamp}.xlsx"
    read_write_excel.write_excel(final_status, final_date_scores, watch_list_path)


if __name__ == "__main__":
    main()
