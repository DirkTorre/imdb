#!/bin/sh

# load the environment (does not work)
# conda init bash
# conda activate ML

# downloading of imdb files
# python download_imdb_files.py

# adding new movies to the list
python add_to_movie_list.py

# creating the movie list
python compile_movie_list.py
