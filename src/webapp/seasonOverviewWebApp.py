from flask import Flask, render_template, request
from bokeh.embed import components

import pyspark                      
from pyspark import SparkContext 
from pyspark.sql import SQLContext
import pyspark.sql.functions as func    # for ETL, data processing on Dataframes

import pandas as pd                     # converting PysparkDF to PandasDF when passing it as a parameter to Bokeh invokes 

from datetime import *                  # for datetime datatype for schema
from dateutil.parser import parse       # for string parse to date

from bokeh.charts import output_file, show                    # creating bar charts, and displaying it
from bokeh.charts.attributes import cat                            # extracting column for 'label' category in bar charts
from bokeh.core.properties import field
from bokeh.io import push_notebook, show, output_notebook          # various output methods for jupyter notebook
from bokeh.models import Legend, LegendItem, HoverTool, ColumnDataSource       # for hover feature, and columnDS
from bokeh.models.glyphs import Rect
from bokeh.palettes import *                                       # brewer color palette
from bokeh.plotting import figure                                  # creating a figure variable

app = Flask(__name__)
sc = SparkContext()
sql = SQLContext(sc)

data_path = "input/csv/"                                # path directory to input csv files
data_opath = "output/csv/"                              # path directory to output csv files


########### Common Module ###########
def get_match_df():
    match_rdd = sc.textFile(data_path + "matches.csv")        # reading csv files into RDD
    match_header = match_rdd.filter(lambda l: "id,season" in l)     # storing the header tuple
    match_no_header = match_rdd.subtract(match_header)              # subtracting it from RDD
    match_temp_rdd = match_no_header.map(lambda k: k.split(','))\
    .map(lambda p: (int(p[0]), p[1],p[2],parse(p[3]).date(),p[4]\
                    ,p[5],p[6],p[7],p[8],p[9]=='1',p[10],int(p[11])\
                    ,int(p[12]),p[13],p[14],p[15],p[16],p[17]))     # Transforming csv file data
    match_df = sql.createDataFrame(match_temp_rdd, match_rdd.first().split(','))  # converting to PysparkDF
    match_df = match_df.orderBy(match_df.id.asc())                                # asc sort by id
    return match_df

def dtype_cast(data, dtype):
	if dtype == "int":
		return int(data)
	elif dtype == "str":
		return str(data)
	elif dtype == "long":
		return long(data)
	elif dtype == "date":
		return parse(data).date()
	else:
		print dtype

def get_color_list(paletteName,numRows):
    return all_palettes[paletteName][numRows]


def get_dropdown_list(srcDF, attr, sort_req, dtype):
    attrDF = srcDF.select(attr).distinct()
    
    if sort_req:
        attrDF = attrDF.orderBy(attr)
        
    attrRange = attrDF.rdd.map(lambda x: dtype_cast(x[0],dtype)).collect()
    return attrRange

########### Season Overview Module ###########
def get_clean_range(tmp_list, sort_req):              # for sanitizing fields
    item_range = []
    for item in tmp_list:
        if item[0]=='"':
            item_range.append(item[1:])
        else:
            item_range.append(item)
    if sort_req:
        item_range.sort()
    return item_range


def get_range(srcDF, seasonNum, attr, distinct_req, sort_req):  # geting a list of range values
    if distinct_req:
        attrDF = srcDF.filter(srcDF.season == seasonNum).select(attr).distinct()
    else:
        attrDF = srcDF.filter(srcDF.season == seasonNum).select(attr)
    
    if sort_req:
        attrDF = attrDF.orderBy(attr)
        
    attrRange = attrDF.rdd.map(lambda x: str(x[0])).collect()
    return attrRange


def get_axis_range(srcDF, seasonNum, attr):                  # get range values for x & y axes
    return [str(x) for x in get_range(srcDF, seasonNum,attr, 1, 1) ]


def display_season_overview(src, seasonNum, yrange, xrange):    # creating and displaying visualizations
    figure_season_overview = figure(title="Season Overview : "+str(seasonNum), tools="hover, save",\
               y_range=yrange, x_range=list(xrange), plot_width=1200, plot_height=500)
    
    figure_season_overview.xaxis.major_label_orientation = 45    # Configuring
    figure_season_overview.yaxis.axis_label = 'Stadium Cities'   # figure
    figure_season_overview.xaxis.axis_label = 'Dates'            # settings
    
    rect = Rect(x="dates", y="cities", width=0.8, height=0.8, fill_alpha=0.8, fill_color="type_color")
    rect_render = figure_season_overview.add_glyph(src, rect)
    
    legend = Legend(items=[ LegendItem(label=field("label"), renderers=[rect_render]) ])
    figure_season_overview.add_layout(legend, 'left')

    figure_season_overview.legend.background_fill_color = "grey"
    figure_season_overview.legend.background_fill_alpha = 0.1
    figure_season_overview.legend.border_line_color = "black"
    
    figure_season_overview.select_one(HoverTool).tooltips = [
                ("Date", "@dates"),
                ("Team1", "@team1"),                             # Configuring
                ("Team2", "@team2"),                             # Hover
                ("Venue", "@venues"),                            # Tool
                ("City", "@cities"),
                ("Winner", "@winners"),
                ("Man of the match","@player_of_match")
            ]
    return figure_season_overview                                                                 # displaying generated visualization
    

def create_figure_season_overview(srcDF, seasonNum):                      # primary module function that defines visualization schema,
    xrange = get_axis_range(srcDF, seasonNum, "date")           # properties, colormaps, axes, and associated data(for hover tool)
    yrange = get_axis_range(srcDF, seasonNum, "city")           # getting x & y axes ranges

    colorMap = {                                        # Colormap mapped to colors based on team jerseys
        ''                              : '#000000',
        'Chennai Super Kings'           : '#EED200',
        'Deccan Chargers'               : '#EA290B',
        'Delhi Daredevils'              : '#0043A8',
        'Gujarat Lions'                 : '#9467BD',
        'Kings XI Punjab'               : '#DB0033',
        'Kochi Tuskers Kerala'          : '#E377C2',
        'Kolkata Knight Riders'         : '#6600DE',
        'Mumbai Indians'                : '#0092CD',
        'Pune Warriors'                 : '#BCBD22',
        'Rajasthan Royals'              : '#B19237',
        'Rising Pune Supergiants'       : '#BCBD22',
        'Royal Challengers Bangalore'   : '#4FC730',
        'Sunrisers Hyderabad'           : '#EA290B'
    }

    src = ColumnDataSource(                             # Defines column data source to be utilized for visualization                       
        data=dict(                                      # using Bokeh libraries
            dates = [str(x) for x in get_range\
                     (srcDF, seasonNum,"date",0,0)],
            venues = [str(x) for x in get_clean_range\
                      (get_range(srcDF, seasonNum,"venue",0,0), 0)],
            cities = get_range(srcDF, seasonNum,"city",0,0),
            team1 = get_range(srcDF, seasonNum,"team1",0,0),
            team2 = get_range(srcDF, seasonNum,"team2",0,0),
            toss_winner = get_range\
            (srcDF, seasonNum,"toss_winner",0,0),
            toss_decision = get_range\
            (srcDF, seasonNum,"toss_decision",0,0),
            result = get_range(srcDF, seasonNum,"result",0,0),
            winners = get_range(srcDF, seasonNum,"winner",0,0),
            win_by_runs = get_range\
            (srcDF, seasonNum,"win_by_runs",0,0),
            win_by_wickets = get_range\
            (srcDF, seasonNum,"win_by_wickets",0,0),
            player_of_match = get_range\
            (srcDF, seasonNum,"player_of_match",0,0),
            umpire1 = get_range(srcDF, seasonNum,"umpire1",0,0),
            umpire2 = get_range(srcDF, seasonNum,"umpire2",0,0),
            umpire3 = get_range(srcDF, seasonNum,"umpire3",0,0),        
            type_color=[colorMap[x] for x in \
                        get_range(srcDF, seasonNum,"winner",0,0)],
            label = [x if x!="" else "Tie" for x in get_range(srcDF, seasonNum,"winner",0,0)]
        )
    )
    return display_season_overview(src, seasonNum, yrange, xrange)


mdf = get_match_df()
seasonList = get_dropdown_list(mdf,"season",1,"int")

# Index page
@app.route('/')
def index():
    # Determine the selected feature
    season = request.args.get("season")
    if season == None:
        season = 2013
    else:
        season = int(season)

    # Create the plot
    plot = create_figure_season_overview(mdf, season)
    
    # Embed plot into HTML via Flask Render
    script, div = components(plot)
    return render_template("seasonOverview.html",script=script,\
            div=div, seasonList=seasonList, season=season)

# With debug=True, Flask server will auto-reload 
# when there are code changes
if __name__ == '__main__':
	app.run(port=5000, debug=True)
