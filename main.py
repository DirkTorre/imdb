from pathlib import Path

import src.imdb.download as download
import src.csv.load_csv as load_csv



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

    date_scores = load_csv.getDateScores(file_path=date_scores_path)
    status = load_csv.getStatus(file_path=status_path)

    



    
    

if __name__ == "__main__":
    main()