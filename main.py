from pathlib import Path
import argparse
import pandas as pd
import os

import src.imdb.download as download
import src.imdb.load as load
import src.read_write.csv as read_write_csv
import src.read_write.excel as read_write_excel
import src.visualization.recommendations as rec


def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Process and generate the watch list",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-u", "--update", action="store_true", help="Download fresh IMDb dataset")
    parser.add_argument("-d", "--dashboard", action="store_true", help="Create movie recommendations dashboard")
    parser.add_argument("-e", "--excel", action="store_true", help="Generate Excel file")
    parser.add_argument("-r", "--reuse", action="store_true", help="Reuse previously generated data")
    
    return parser.parse_args()


def define_paths():
    """Defines file paths used in processing."""
    base_path = Path(__file__).parent.resolve()
    return {
        "imdb_files": base_path / "data" / "downloads",
        "sheets": base_path / "data" / "sheets",
        "status_pickle": base_path / "data" / "status.pickle",
    }


def download_imdb_data(imdb_files_path):
    """Downloads IMDb dataset if needed."""
    files_to_download = {"title.basics": True, "title.ratings": True}
    download.Download(imdb_files_path, files_to_download).download_files()
    print("Files downloaded.")


def load_data(sheets_path, imdb_files_path):
    """Loads necessary data for processing."""
    date_scores = read_write_csv.get_date_scores(sheets_path / "date_scores.csv")
    status = read_write_csv.get_status(sheets_path / "status.csv")

    needed_indices = date_scores.index.union(status.index)
    
    title_basics = load.get_title_basic(needed_indices, imdb_files_path / "title.basics.tsv.gz")
    title_ratings = load.get_title_rating(needed_indices, imdb_files_path / "title.ratings.tsv.gz")

    return date_scores, status, title_basics, title_ratings


def generate_final_status(status, title_ratings, title_basics):
    """Generates final status data for movies."""
    return (
        status.join(title_ratings)
        .join(title_basics)
        .sort_values(["watched", "priority", "averageRating"], ascending=[True, False, False])
    )


def main():
    """Main function to process and generate the watch list."""
    args = parse_arguments()
    paths = define_paths()

    imdb_files_path = paths["imdb_files"]
    sheets_path = paths["sheets"]
    status_path_temp = paths["status_pickle"]

    # Check if IMDb files are available
    no_tit_bas = not os.path.exists(imdb_files_path / "title.basics.tsv.gz")
    no_tit_rat = not os.path.exists(imdb_files_path / "title.ratings.tsv.gz")
    imdb_files_not_available = no_tit_bas or no_tit_rat

    if args.update or imdb_files_not_available:
        download_imdb_data(imdb_files_path)

    if args.excel or not args.reuse:
        print("Assembling data...")
        date_scores, status, title_basics, title_ratings = load_data(sheets_path, imdb_files_path)
        
        final_status = generate_final_status(status, title_ratings, title_basics)
        final_status.to_pickle(status_path_temp)

    if args.reuse:
        print("Reusing data...")
        final_status = pd.read_pickle(status_path_temp)

    if args.excel:
        print("Generating Excel file...")
        final_date_scores = date_scores.join(title_basics[["primaryTitle", "originalTitle", "startYear"]]).sort_values("date")
        read_write_excel.write_excel(final_status, final_date_scores, sheets_path / "watch_list.xlsx")
        print(f"Excel file created at: {sheets_path / 'watch_list.xlsx'}")

    if args.dashboard:
        print("Creating movie recommendation visualization...")
        rec.create_movie_recommendations(final_status)


if __name__ == "__main__":
    main()
