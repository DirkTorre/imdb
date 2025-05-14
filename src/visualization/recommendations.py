from bokeh.models import ColumnDataSource, CDSView, GroupFilter, HoverTool, Tabs, TabPanel, TableColumn, GroupingInfo, DataCube, Div
from bokeh.plotting import figure, show
from bokeh.layouts import column
import pandas as pd

def create_movie_recommendations(final_status):
    """Generate Bokeh visualizations for unwatched movies"""
    print("Creating movie recommendations")

    final_status["priority"] = final_status["priority"].map({True: "y", False: "n"})
    source = ColumnDataSource(final_status[~final_status["watched"]])

    filter_priority = GroupFilter(column_name='priority', group="y")
    view_priority = CDSView(filter=filter_priority)
    view_all = CDSView()

    tooltips = [("title", "@primaryTitle"), ("year", "@startYear")]

    def create_figure(title, view):
        fig = figure(title=title, x_axis_label="Number of Votes", y_axis_label="Average Rating",
                     width=500, height=500, y_range=(0, 10), tooltips=tooltips)
        fig.circle(x="numVotes", y="averageRating", radius=5000, source=source, view=view)
        
        hover = fig.select_one(HoverTool)
        hover.point_policy = "follow_mouse"
        hover.tooltips = tooltips
        return fig

    fig_all = create_figure("Unwatched movies", view_all)
    fig_prio = create_figure("Unwatched priority movies", view_priority)

    all_tabs = Tabs(tabs=[
        TabPanel(child=fig_prio, title="Priority Movies"),
        TabPanel(child=fig_all, title="All Movies"),
    ])

    genre_columns = [col for col in final_status.columns if "genre" in col]
    genres = (final_status[genre_columns]
              .replace(False, pd.NA).stack().reset_index()
              .drop(0, axis=1).rename(columns={"level_1": "genre"}))
    
    genres["genre"] = genres["genre"].str.replace("genre_", "")
    genres = genres.set_index("tconst")

    display_status = final_status[~final_status["watched"]].drop(columns=genre_columns)
    display_status = display_status.join(genres).groupby("genre").head(10).sort_values(["genre", "averageRating"], ascending=[True, False])
    display_status = display_status[["averageRating", "primaryTitle", "startYear", "genre"]].round({"averageRating": 1})

    top10 = ColumnDataSource(display_status)
    columns = [TableColumn(field=field, title=title) for field, title in [
        ("tconst", "tconst"), ("genre", "Genre"), ("primaryTitle", "Title"),
        ("startYear", "Year"), ("averageRating", "Rating")]]
    
    grouping = [GroupingInfo(getter="genre")]
    target = ColumnDataSource(data=dict(row_indices=[], labels=[]))
    data_cube = DataCube(source=top10, columns=columns, grouping=grouping, target=target, width=500, height=500)

    header1 = Div(text='<h1 style="text-align: center">Visualization of Unwatched Movies</h1>')
    header2 = Div(text='<h1 style="text-align: center">Collapsible Table of Top 10 unwatched Movies per Genre</h1>')

    show(column(header1, all_tabs, header2, data_cube))
