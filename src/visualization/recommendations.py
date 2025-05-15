from bokeh.models import (
    CDSView,
    ColumnDataSource,
    DataCube,
    DataTable,
    Div,
    GroupFilter,
    GroupingInfo,
    HoverTool,
    HTMLTemplateFormatter,
    OpenURL,
    Tabs,
    TabPanel,
    TableColumn,
    TapTool,
)
from bokeh.plotting import figure, show
from bokeh.layouts import column
import pandas as pd


def create_movie_recommendations(final_status):
    """Generate Bokeh visualizations for unwatched movies"""
    print("Creating movie recommendations")

    final_status["priority"] = final_status["priority"].map({True: "y", False: "n"})
    final_status["url"] = (
        "https://www.imdb.com/title/" + final_status.index.astype(str) + "/"
    )
    source = ColumnDataSource(final_status[~final_status["watched"]])

    filter_priority = GroupFilter(column_name="priority", group="y")
    view_priority = CDSView(filter=filter_priority)
    view_all = CDSView()

    tooltips = [("title", "@primaryTitle"), ("year", "@startYear")]

    def create_figure(title, view, color):
        fig = figure(
            title=title,
            x_axis_label="Number of Votes",
            y_axis_label="Average Rating",
            width=1500,
            height=500,
            y_range=(0, 10),
            tooltips=tooltips,
            tools="tap",
        )
        fig.circle(
            x="numVotes",
            y="averageRating",
            radius=3000,
            alpha=0.5,
            source=source,
            view=view,
            color=color
        )

        hover = fig.select_one(HoverTool)
        hover.point_policy = "follow_mouse"
        hover.tooltips = tooltips

        taptool = fig.select(type=TapTool)
        taptool.callback = OpenURL(url="@url")

        return fig

    fig_all = create_figure("Unwatched movies", view_all, "blue")
    fig_prio = create_figure("Unwatched priority movies", view_priority, "green")

    all_tabs = Tabs(
        tabs=[
            TabPanel(child=fig_prio, title="Priority Movies"),
            TabPanel(child=fig_all, title="All Movies"),
        ]
    )

    genre_columns = [col for col in final_status.columns if "genre" in col]
    genres = (
        final_status[genre_columns]
        .replace(False, pd.NA)
        .stack()
        .reset_index()
        .drop(0, axis=1)
        .rename(columns={"level_1": "genre"})
    )

    genres["genre"] = genres["genre"].str.replace("genre_", "")
    genres = genres.set_index("tconst")

    display_status = final_status[~final_status["watched"]].drop(columns=genre_columns)
    display_status = (
        display_status.join(genres)
        .groupby("genre")
        .head(10)
        .sort_values(["genre", "averageRating"], ascending=[True, False])
    )
    display_status = display_status[
        ["averageRating", "primaryTitle", "startYear", "genre"]
    ].round({"averageRating": 1})

    top10 = ColumnDataSource(display_status)
    columns = [
        TableColumn(field=field, title=title)
        for field, title in [
            ("genre", "Genre"),
            ("primaryTitle", "Title"),
            ("startYear", "Year"),
            ("averageRating", "Rating"),
            ("tconst", "tconst"),
        ]
    ]

    grouping = [GroupingInfo(getter="genre")]
    target = ColumnDataSource(data=dict(row_indices=[], labels=[]))
    data_cube = DataCube(
        source=top10,
        columns=columns,
        grouping=grouping,
        target=target,
        width=1500,
        height=500,
    )

    header1 = Div(
        text="""<h1 style="text-align: center">Visualization of Unwatched Movies</h1>
                <ul>
                    <li>Use tabs to switch info.</li>
                    <li>Hover over data points to view movie info.</li>
                    <li>Click data point to go to IMDb.</li>
                </ul>"""
    )
    header2 = Div(
        text="""<h1 style="text-align: center">Top 10 Unwatched Movies per Genre</h1>
        Press the + icon at the genre to unfold the movies."""
    )

    show(column(header1, all_tabs, header2, data_cube))
