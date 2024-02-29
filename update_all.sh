#!/bin/sh

# load the environment (does not work)
# conda init bash
# conda activate ML

start=`date +%s`

# # downloading of imdb files
# python download_imdb_files.py
# end=`date +%s`
# runtime=$((end-start))
# echo "download complete. runtime: ${runtime} seconds"

# adding new movies to the list
python add_to_movie_list.py
end=`date +%s`
runtime=$((end-start))
echo "adding to list complete. runtime: ${runtime} seconds"

# creating the movie list
python compile_movie_list.py
end=`date +%s`
runtime=$((end-start))
echo "compiling excel complete. runtime: ${runtime} seconds"
