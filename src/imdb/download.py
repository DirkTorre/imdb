from dataclasses import dataclass
from pathlib import Path
from pypdl import Pypdl


class Download:
    """
    A module to download files from IMDb.

    Parameters
    ----------
    files_path : Path | str
        The local directory where files should be downloaded.
    files_to_download : dict[str, bool], optional
        A dictionary mapping file names to whether they should be downloaded.
        Defaults to:
        {
            "title.crew": False,
            "title.basics": False,
            "title.ratings": False,
            "title.principals": False,
            "name.basics": False,
        }
    """
    
    @dataclass
    class File:
        """Represents a file to be downloaded from IMDb."""
        
        imdb_base_url: str = "https://datasets.imdbws.com/"
        name: str = ""
        base_path: Path = Path()
        
        def __post_init__(self):
            """Ensure `base_path` is a Path object."""
            self.base_path = Path(self.base_path)
        
        def get_imdb_url(self) -> str:
            """Returns the URL of the file on the IMDb server."""
            return f"{self.imdb_base_url}{self.name}.tsv.gz"

        def get_local_file_path(self) -> Path:
            """Returns the local path for the downloaded raw file."""
            return self.base_path / f"{self.name}.tsv.gz"
        
        def get_local_directory(self) -> Path:
            """Returns the local directory where the file is stored."""
            return self.base_path

    def __init__(self, files_path: Path | str, files_to_download: dict[str, bool] = None):
        self.files_path = Path(files_path)
        self.files_to_download = files_to_download or {
            "title.crew": False,
            "title.basics": False,
            "title.ratings": False,
            "title.principals": False,
            "name.basics": False,
        }
        self.files = {}
        for key, value in self.files_to_download.items():
            if value:
                self.files[key] = self.File(name=key, base_path=files_path)

    def download_files(self):
        """Downloads the selected files from IMDb."""
        tasks = []

        for file in self.files.values():
            # Ensure the directory exists
            try:
                file.get_local_directory().mkdir(parents=True, exist_ok=True)
            except OSError as e:
                print(f"Error creating directory {file.get_local_directory()}: {e}")

            task = {
                "url": file.get_imdb_url(),
                "file_path": str(file.get_local_file_path()),
            }
            tasks.append(task)

        # Start downloading files
        downloader = Pypdl()
        downloader.start(tasks=tasks, multisegment=True, segments=4, overwrite=True)
