from src.imdb.download import Download
from pathlib import Path


def main():
    imdb_files_path = Path(__file__).parent.resolve() / "data/downloads"
    files_to_download = {"title.basics": False, "title.ratings": True}
    
    download = Download(files_path=imdb_files_path, files_to_download=files_to_download)
    download.download_files()
    
    

if __name__ == "__main__":
    main()