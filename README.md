# IMDb data project.

## Project to learn new Python skills

To guide my project I set some goals that can help me with my hobby.
The project was initiated because I am slowly running out of interesting movies to watch.

### Goals

1. Add IMDb data to my movie list.
2. Make process of fully updating the movie list automated.
3. Use ML to find new interesting movies.

## Files
<b>download_imdb_files.py</b>

- Downloads fresh copy of the iMDb database.
- Processes files and saves them as parquet.

<b>compile_movie_list.py</b>

- Takes data/handcrafted/raw_status.xlsx
- Adds info from IMDb parquet files.
- Generates films_data/generated/films_reading.xlsx

<b>cheat_sheet.txt</b>

- Contains reference code and tips.

<b>Folder: data</b>

Contains handcrafted files and will contain downloaded files at executing download_imdb_files.py.

<b>Folder: database</b>

Contains ideas on how to order the data

<b>Folder: old</b>

Contains testing scripts and scripts with attempts at data processing, cleaning, mining, and ML.
It containes outdated scripts and will be cleaned in the long run.
