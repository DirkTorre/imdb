import pytest
from download import Download
from pathlib import Path
import os
"""
What to test:
    - are the right files downloaded at the right place?
    - are the downloaded files non-empty?
    - are the urls right?
    - are the download folders made if there are none?



"""

# class TestDownload:
#     def test_folder_made(self, tmpdir):
#         temp_path = tmpdir.mkdir("test_files")
#         files_to_download = {"title.ratings": True}
#         download = Download(files_path=temp_path, files_to_download=files_to_download)
#         download.download_files()
#         assert os.path.exists(temp_path)

