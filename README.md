# IMDb project

A project to create a nice spreadsheet and dashboard of information about movies I want to watch.
The spreadsheet is used to quickly find a movie on movie night with family or friends.


## Goals of the program

- Updating an excel file with information about movies I have and want to watch.
- Excel file should filled with information about the movie using IMDB data
- Excel file should have 2 tabs:
    - One with watch status and info of all my movies
    - One with states the dates I watched a movie
- Create a visualization with movie recommendations from my list.

## user guide

Install the environment (see the env commands section).

Add movie info of your watch list to `/data/downloads/sheets/status.csv`.
A o represent not and 1 represents yes/watched/etc.
Only add the id of the movie, which can be found at the [imdb.com](https://www.imdb.com/title/tt0057012/?ref_=nv_sr_srsg_0_tt_8_nm_0_in_0_q_dr%2520strangel) in the url of the movie.

Add dates (and scores) of watched movies to `/data/downloads/sheets/status.csv`.
Date format must be `<year>-<month>-<day>`.

Run the script in the main folder: `uv run main -ed`.
IMDBb data is always downloaded when not available on disk.

It generates an excel file and shows the dashboard in a new browser tab.
The `main.html` file can be used to share the dashboard.
The excel file `/data/sheets/watch_list_<date+time>` can be used to sort and filter the data to quickly find a movie you want to watch.

### flags

The main command is : `uv run main.py`.

arguments:

- `-u` Force an update of IMDBb data.
- `-e` Creates an excel file from available data.
- `-d` Creates a dashboard from available data in a new web browser tab.
    + `-r` Force reuse of previous generated data. Must be combined with `-e` or `-d`.

## kanban board

Keeping track of tasks if managed with the VSCode extension Kanban made by Marcel J. Kloubert.

## env commands 

This project uses uv as a package manager.
Information on how to install uv [can be found here](https://docs.astral.sh/uv/getting-started/installation/).

To create or sync the environment with the dependencies use:

```bash
uv sync
```

To run the project use:

```bash
uv run main.py <flags>
```


To run any script use:

```bash
uv run <path + file>
```