# IMDb Movie Tracker

A Python project for collecting and visualizing IMDb movie data, creating a structured spreadsheet and an interactive dashboard. This tool helps you quickly find a movie for movie nights with family or friends.

## Features

- Maintain an **Excel spreadsheet** with movie details and watch history.
- Automatically populate movie information using **IMDb data**.
- Organize movies into **two tabs**:
  - **Watch List**: Status and details of all movies.
  - **Watch History**: Dates and scores of watched movies.
- Generate an interactive **dashboard** with personalized movie recommendations.

## Setup

### 1. Install Dependencies

This project uses `uv` as a package manager. Install `uv` by following the instructions [here](https://docs.astral.sh/uv/getting-started/installation/).

To create or sync the environment, run:

```bash
uv sync
```

### 2. Prepare Your Movie List

1. Add movie IDs to your watch list in `/data/downloads/sheets/status.csv`.
   - `0` → Not watched  
   - `1` → Watched  
   - Only include the **IMDb ID** (found in the movie’s URL, e.g., `tt0057012`).
   
2. Log watched movies in `/data/downloads/sheets/status.csv` with:
   - **Date format**: `YYYY-MM-DD`
   - **Optional score**: Your personal rating.

### 3. Run the Program

Execute the main script:

```bash
uv run main.py -ed
```

IMDb data is automatically downloaded if not available on disk.

The script will:
- Generate an **Excel file** (`/data/sheets/watch_list.xlsx`) for sorting and filtering movies.
- Open the **dashboard** (`main.html`) in a new browser tab for an interactive view.

## Command Line Flags

Main command:

```bash
uv run main.py <flags>
```

Available flags:
- `-u` → Force update of IMDb data.
- `-e` → Generate an Excel file from available data.
- `-d` → Create a dashboard in a web browser.
  - `-r` → Reuse previously generated data (must be combined with `-e` or `-d`).

## Kanban Board

Task management is handled with the [Kanban extension for VSCode](https://marketplace.visualstudio.com/items?itemName=mkloubert.vscode-kanban) by Marcel J. Kloubert.