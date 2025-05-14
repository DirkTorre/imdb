from pathlib import Path
from datetime import datetime
import argparse

import src.imdb.download as download
import src.imdb.load as load
import src.read_write.csv as read_write_csv
import src.read_write.excel as read_write_excel

from bokeh.models import (ColumnDataSource, HoverTool, DataTable, TableColumn, DataCube, GroupingInfo, GroupFilter, CDSView, TabPanel, Tabs, Div)
from bokeh.plotting import figure, show
from bokeh.layouts import column
import pandas as pd

def main() -> None:
    """Main function to process and generate the watch list."""

    parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--download", action="store_true", help="Downloads a fresh IMDb dataset")
    parser.add_argument("-r", "--recommendations", action="store_true", help="Creates a movie recommendations dashboard")
    args = vars(parser.parse_args())

    # Define file paths
    base_path = Path(__file__).parent.resolve()
    imdb_files_path = base_path / "data" / "downloads"
    sheets_path = base_path / "data" / "sheets"

    if args["download"]:
        # Download IMDb files
        files_to_download = {"title.basics": False, "title.ratings": True}
        download.Download(imdb_files_path, files_to_download).download_files()
        print("Files downloaded")

    print("Assembling data")
    # Import movie status data
    date_scores_path = sheets_path / "date_scores.csv"
    status_path = sheets_path / "status.csv"

    date_scores = read_write_csv.get_date_scores(date_scores_path)
    status = read_write_csv.get_status(status_path)

    # Retrieve movie info
    needed_indices = date_scores.index.union(status.index)
    title_basics_path = imdb_files_path / "title.basics.tsv.gz"
    title_ratings_path = imdb_files_path / "title.ratings.tsv.gz"

    title_basics = load.get_title_basic(needed_indices, title_basics_path)
    title_ratings = load.get_title_rating(needed_indices, title_ratings_path)

    # Prepare final status data
    final_status = (
        status.join(title_ratings)
        .join(title_basics)
        .sort_values(
            ["watched", "priority", "averageRating"], ascending=[True, False, False]
        )
    )

    # Prepare final date scores data
    final_date_scores = date_scores.join(
        title_basics[["primaryTitle", "originalTitle", "startYear"]]
    ).sort_values("date")

    # Write data to an Excel file
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    watch_list_path = sheets_path / f"watch_list_{timestamp}.xlsx"
    read_write_excel.write_excel(final_status, final_date_scores, watch_list_path)
    print(f"Created excel file can be found at: {watch_list_path}")
    
    if args["recommendations"]:
        # TODO: can be removed?
        status_path_temp = base_path / "data" / "status.pickle"
        date_scores_path_temp = base_path / "data" / "date_scores.pickle"
        final_status.to_pickle(status_path_temp)
        final_date_scores.to_pickle(date_scores_path_temp)

        print("Creating movie recommendations")
        final_status["priority"] = final_status["priority"].map({True: "y", False: "n"})

        source = ColumnDataSource(final_status[~final_status["watched"]])
        # filtered_source = ColumnDataSource(final_status[~final_status["watched"] & final_status["priority"]])

        
        filter_priority = GroupFilter(column_name='priority', group="y")
        view_priority = CDSView(filter=filter_priority)
        view_all = CDSView()

        TOOLTIPS = [
            ("title", "@primaryTitle"),
            ("year", "@startYear")
        ]

        # Create a Bokeh figure
        fig_all = figure(title="Unwatched movies",
                x_axis_label="Number of Votes",
                y_axis_label="Average Rating",
                width=500, height=500, y_range=(0,10),
                tooltips=TOOLTIPS)

        # Add scatter plot
        fig_all.circle(x="numVotes", y="averageRating", radius=5000, source=source, name="all", view=view_all)

        # Create a Bokeh figure
        fig_prio = figure(title="Unwatched priority movies",
                x_axis_label="Number of Votes",
                y_axis_label="Average Rating",
                width=500, height=500, y_range=(0,10),
                tooltips=TOOLTIPS)

        # Add scatter plot
        fig_prio.circle(x="numVotes", y="averageRating", radius=5000, source=source, name="prio", view=view_priority)

        # fig.select(name="temp")

        hover_all = fig_all.select_one(HoverTool)
        hover_all.point_policy = "follow_mouse"
        # And we reference the mayor in the tooltip
        hover_all.tooltips = [
            ("title", "@primaryTitle"),
            ("year", "@startYear")
        ]

        hover_prio = fig_prio.select_one(HoverTool)
        hover_prio.point_policy = "follow_mouse"
        # And we reference the mayor in the tooltip
        hover_prio.tooltips = [
            ("title", "@primaryTitle"),
            ("year", "@startYear")
        ]


        all_tabs = Tabs(tabs=[
            TabPanel(child=fig_prio, title="priority movies"),
            TabPanel(child=fig_all, title="all movies"),
        ])

        

        
        genre_columns = [ x for x in list(final_status.columns) if "genre" in x ]

        genres = final_status[genre_columns].replace(False, pd.NA).stack().reset_index().drop(0, axis=1).rename(columns={"level_1": "genre"})
        genres["genre"] = genres["genre"].str.replace("genre_","")
        genres = genres.set_index("tconst")

        display_status = final_status[~final_status["watched"]].drop(columns=genre_columns)
        display_status = display_status.join(genres).groupby("genre").head(10).sort_values(["genre", "averageRating"], ascending=[True, False])
        display_status = display_status[["averageRating", "primaryTitle", "startYear", "genre"]].round({"averageRating": 1})

        top10 = ColumnDataSource(display_status)
        columns = [
                TableColumn(field="tconst", title="tconst"),
                TableColumn(field="genre", title="genre"),
                TableColumn(field="primaryTitle", title="title"),
                TableColumn(field="startYear", title="year"),
                TableColumn(field="averageRating", title="rating"),
            ]

        grouping = [
            GroupingInfo(getter="genre")
        ]

        target = ColumnDataSource(data=dict(row_indices=[], labels=[]))

        data_cube = DataCube(source=top10, columns=columns, grouping=grouping, target=target, width=500, height=500)

        header1 = Div(text='<h1 style="text-align: center">Visualization of unwatched movies</h1>')
        header2 = Div(text='<h1 style="text-align: center">Collapsable table of top 10 movies per genre</h2>')
        
        show(column(header1, all_tabs, header2, data_cube))

if __name__ == "__main__":
    main()
