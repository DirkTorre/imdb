# How I would redo the project

I'm going to drop the machine learning element.
If I want to learn more about machine learning I'm better off using a smaller and less messy and complicated dataset.
If I want to learn more about databases, I just should start a new project.

I's probably better to drop the SnakeMake element and use it for a different project.


## Whithout database (realistic version)

1. Make a file structure using the Python package cookiecutter - Data Science.
2. Make a script that downloads the IMDb files (use logging)
3. Write better pandas (with pipes and modules) to wrangle the data into good parquet files
4. Create scripts that can generate the needed files for the movie id's that I want.
    1. Create a excel file for onedrive
    2. Create parquet or feather files for data analysis.
5. Create a promt that asks if you want to:
    1. Update the score list with my scores. (Make sure it checks if no vi codes are used.)
    2. Download a fresh copy from IMDb.
    3. Create the excell file.
    4. Create the parquet for analysis
    5. Update the dashboard
    6. A combination for all of the above.
6. Create modules to analyse the data.
7. Use these modules in notebooks to analyse the data.
8. Create a nice dashboard that gives some stats and movie recommendations.

## Overkill continuation
The following can be added to the previous.

1. Create a little GUI for:
    - Updating the IMDb files.
    - Adding movies and scores I have seen.
    - Generate and showing the dashboard.
    - Creating the excel file for onedrive.
2. Use SnakeMake to make the data analysis pipeline automated.
3. Research if my experience theory is correct.



## With database (total overkill)
This is nice, but at this point it's better to start a new project where I can use a database.

1. Make a file structure using the Python package cookiecutter - Data Science.
2. Make a script that downloads the IMDb files
3. Write better pandas (with pipes and modules) to wrangle the data into database ready format
4. Create a database to store all the data in, including watched movies.
5. Make a backup of my movies and scores to my NAS.
6. Create a script that can update the local SQL database on command.
7. Create a SQL scripts that can create the needed files for the movie id's that I want.
    1. 2 types of scripts: 1 for onedrive backup, 1 for data analysis.
    2. Store these files as parque files of feather, depends.
8. Create modules to analyse the data.
9. Use these modules in notebooks to analyse the data.
10. Use SnakeMake to make the data analysis pipeline automated.
11. Create a nice dashboard that gives some stats and movie recommendations.
12. Create a little GUI for:
    1. Updating the IMDb database.
    2. Adding movies and scores I have seen.
    3. Creating and restoring a backup to/from my NAS.
    4. Generate and showing the dashboard.
    5. Creating the excel file for onedrive.
13. Research if my experience theory is correct.
