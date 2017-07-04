from flask import Flask, render_template, request
from src.webapp.corefuncs import *

import pygal    
from pygal.style import DefaultStyle

from bokeh.charts import Bar            # creating bar charts, and displaying it
from bokeh.charts.attributes import cat                            # extracting column for 'label' category in bar charts
from bokeh.core.properties import field
from bokeh.embed import components
from bokeh.models import Legend, LegendItem, HoverTool, ColumnDataSource       # for hover feature, and columnDS
from bokeh.models import Range1d                               # brewer color palette
from bokeh.models.glyphs import Rect
from bokeh.plotting import figure                                  # creating a figure variable

# app = Flask(__name__)

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


########### Overall Standings Module ###########
def get_overall_ranks_df(season_num, srcDF):
    overallRanking = srcDF.filter(srcDF.season == season_num)\
                    .groupBy("winner").count().orderBy("count",ascending=0) # extracting required columns into another DF
    
    overallRanking = overallRanking.filter("winner != '' ")                          # Deleting records of tied matches
    overallRanking = overallRanking.selectExpr("winner as Team", "count as Wins")   # Renaming columns
    return overallRanking


def create_figure_overall_ranks(srcDF, season_num):
    overallRanksDF = get_overall_ranks_df(season_num, srcDF)
    overallRankspDF = overallRanksDF.toPandas()
    clr = get_color_list('Viridis', overallRanksDF.count())  # Brewing color hex values for each tuple('team')
    figureOverallRanks = Bar(overallRankspDF, values="Wins", color="Team",\
                    palette=clr, label=cat(columns="Team", sort=False),\
                    xgrid=True, xlabel="Team", ylabel="Wins",\
                    title="Overall Standings " + str(season_num),\
                    legend='top_right', plot_width=950, bar_width=0.6)   # generating bar chart
    return figureOverallRanks


########### Performance Consistency Module ###########
def get_consistency_df(srcDF, season_lbound, season_ubound):    
    consistency_df = srcDF.select("season","winner")\
    .groupBy("season","winner").count().orderBy("winner")                  # extracting required columns 

    consistency_df = consistency_df.filter("winner!='' ")                  # filtering out tied matches records
    cond1 = func.col("season") >= season_lbound 
    cond2 = func.col("season") <= season_ubound
    consistency_df = consistency_df.filter(cond1 & cond2) 
    return consistency_df


def get_constraints(srcDF):
    # constraint : teams that haven't played more than three season aren't considered
    constraint_df = srcDF.groupBy("winner","season")\
                .count().orderBy("winner")                                 # extracting list of season-wise winner teams
 
    constraint_df = constraint_df.groupBy("winner").count()\
                .filter("count>3 and winner!='' ")                       # filtering out teams that don't satisfy constraint
    return constraint_df


def filter_using_constraints(consistency_df, constraint_list):
    consistency_df = consistency_df.where(func.col("winner")\
                    .isin(constraint_list))                                # applying the constraint list
    
    consistency_df = consistency_df.groupBy("winner")\
                    .agg(func.stddev_pop("count").alias("stddev"),\
                    func.sum("count").alias("total_wins"))\
                    .orderBy("stddev","total_wins")                        # calculating the performance consistency
    return consistency_df


def calc_consistency(consistency_df):
    consistency_df = consistency_df.withColumn("final_deviations",\
                    ((10-consistency_df.stddev)/10)*100)\
                    .orderBy("final_deviations", ascending=False)          # scaling to appropriate scale
        
    consistency_df = consistency_df.selectExpr("winner as Teams",\
                    "final_deviations as Consistency")    
    return consistency_df


def create_figure_performance_consistency(srcDF, season_lbound = 2008, season_ubound = 2016):
    resultDF = get_consistency_df(srcDF, season_lbound, season_ubound)      # extracting required columns 
    constraints_df = get_constraints(srcDF)
    constraints_list = [i.winner for i in constraints_df.collect()]          # storing a list of filtered teams
    resultDF = filter_using_constraints(resultDF, constraints_list)
    resultDF = calc_consistency(resultDF)

    resultpDF = resultDF.toPandas()
    clr = get_color_list("RdYlGn", resultDF.count())

    figurePerformanceConsistency = Bar(resultpDF,\
            values="Consistency",\
            color="Teams", palette=clr,\
            label=cat(columns="Teams", sort=False),\
            xlabel="Teams", ylabel="Win Consistency %age",\
            title="IPL Performance Consistencies "\
            +str(season_lbound)+"-"+str(season_ubound),
            legend='top_right', plot_width=950, bar_width=0.6)

    figurePerformanceConsistency.y_range = Range1d(60,100)
    return figurePerformanceConsistency


########### PlayerPerformance Module ###########
def get_fielder_ratings2():
    fieldDF = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"fielder.csv"))          #computing the fielder's overall maximum
    toIntfunc = udf(lambda x: int(x),IntegerType())                                                                         #converting the column "overall" to integer.
    field_max_overall = int (fieldDF.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])      #droping the duplicate rows having fielder, overall and ratings common.
    field2 = fieldDF.dropDuplicates(['fielder','overall','ratings'])                                                          #calcuating the overall ratings of every fielder relative to the maximum
    fielder_ratings = field2.withColumn('ratings_overall', (fieldDF.overall / field_max_overall*100)).sort("overall",ascending=0) #reading the batsman.csv file
    fielder_ratings2 = fielder_ratings.withColumn("ratings_overall",toIntfunc(fielder_ratings['ratings_overall']))
    return fielder_ratings2


def avg_bat():
    bat = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"batsman.csv"))            #toIntfunc is a function which converts the values to integer.
    toIntfunc = udf(lambda x: int(x),IntegerType())                                                                         #converting the column "overall" to integer.
    bat2 = bat.withColumn("overall",toIntfunc(bat['overall']))                                                              #computing the batsman's overall maximum
    bat_max_overall = int (bat2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])         #calcuating the overall ratings of every batsman relative to the maximum
    batsman_ratings = bat2.withColumn('ratings_overall', (bat2.overall / bat_max_overall*100)).sort("overall",ascending=0)  #reading the bowler.csv file
    batsman_ratings2 = batsman_ratings.withColumn("ratings_overall",toIntfunc(batsman_ratings['ratings_overall']))
    batsman_avg = batsman_ratings2.agg(func.avg(func.col('ratings_overall')))
    x = batsman_avg.collect()[0][0]
    return round(x,2)


def avg_bowl():
    bowl = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"bowler.csv"))            #converting the column "overall" to integer.
    toIntfunc = udf(lambda x: int(x),IntegerType())                                                                         #converting the column "overall" to integer.
    bowl2 = bowl.withColumn("overall",toIntfunc(bowl['overall']))                                                           #computing the bowler's overall maximum
    bowl_max_overall = int (bowl2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])       #calcuating the overall ratings of every bowler relative to the maximum
    bowler_ratings = bowl2.withColumn('ratings_overall', (bowl2.overall / bowl_max_overall*100)).sort("overall",ascending=0)#converting the new overall ratings to integer.
    bowler_ratings2 = bowler_ratings.withColumn("ratings_overall",toIntfunc(bowler_ratings['ratings_overall']))
    bowler_avg = bowler_ratings2.agg(func.avg(func.col('ratings_overall')))
    x = bowler_avg.collect()[0][0]
    return round(x,2)


def avg_field():
    fieldDF = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"fielder.csv"))          #computing the fielder's overall maximum
    field2 = fieldDF.dropDuplicates(['fielder','overall','ratings'])                                                          #calcuating the overall ratings of every fielder relative to the maximum
    field_max_overall = int (fieldDF.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])      #droping the duplicate rows having fielder, overall and ratings common.
    toIntfunc = udf(lambda x: int(x),IntegerType())                                                                         #converting the column "overall" to integer.
    fielder_ratings = field2.withColumn('ratings_overall', (fieldDF.overall / field_max_overall*100)).sort("overall",ascending=0) #reading the batsman.csv file
    fielder_ratings2 = fielder_ratings.withColumn("ratings_overall",toIntfunc(fielder_ratings['ratings_overall']))
    fielder_avg = fielder_ratings2.agg(func.avg(func.col('ratings_overall')))
    x = fielder_avg.collect()[0][0]
    return round(x,2)


'''
create_figure in a method to create the visualization, taking "player" as an argument 
which would be passed through a dropdown select menu.
'''
def create_figure_player_performance(player):
    fieldDF = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"fielder.csv"))          #computing the fielder's overall maximum
    field_max_overall = int (fieldDF.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])      #droping the duplicate rows having fielder, overall and ratings common.
    field2 = fieldDF.dropDuplicates(['fielder','overall','ratings'])                                                          #calcuating the overall ratings of every fielder relative to the maximum
    fielder_ratings = field2.withColumn('ratings_overall', (fieldDF.overall / field_max_overall*100)).sort("overall",ascending=0) #reading the batsman.csv file
    bat = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"batsman.csv"))            #toIntfunc is a function which converts the values to integer.
    toIntfunc = udf(lambda x: int(x),IntegerType())                                                                         #converting the column "overall" to integer.
    bat2 = bat.withColumn("overall",toIntfunc(bat['overall']))                                                              #computing the batsman's overall maximum
    bat_max_overall = int (bat2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])         #calcuating the overall ratings of every batsman relative to the maximum
    batsman_ratings = bat2.withColumn('ratings_overall', (bat2.overall / bat_max_overall*100)).sort("overall",ascending=0)  #reading the bowler.csv file
    bowl = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"bowler.csv"))            #converting the column "overall" to integer.
    bowl2 = bowl.withColumn("overall",toIntfunc(bowl['overall']))                                                           #computing the bowler's overall maximum
    bowl_max_overall = int (bowl2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])       #calcuating the overall ratings of every bowler relative to the maximum
    bowler_ratings = bowl2.withColumn('ratings_overall', (bowl2.overall / bowl_max_overall*100)).sort("overall",ascending=0)#converting the new overall ratings to integer.
    bowler_ratings2 = bowler_ratings.withColumn("ratings_overall",toIntfunc(bowler_ratings['ratings_overall']))
    fielder_ratings2 = fielder_ratings.withColumn("ratings_overall",toIntfunc(fielder_ratings['ratings_overall']))
    batsman_ratings2 = batsman_ratings.withColumn("ratings_overall",toIntfunc(batsman_ratings['ratings_overall']))

    batsman_name = batsman_ratings2.filter(batsman_ratings2.batsman == player)
    bowler_name = bowler_ratings2.filter(bowler_ratings2.bowler == player)
    fielder_name = fielder_ratings2.filter(fielder_ratings2.fielder == player)
    bat_avg=avg_bat()
    bowl_avg=avg_bowl()
    field_avg=avg_field()

    if batsman_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        bat = bat_avg
    else:
        bat = int(batsman_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0])
        
    if bowler_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        bowl = bowl_avg
    else:
        bowl = int(bowler_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0])
        
    if fielder_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        field = field_avg
    else:
        field = int(fielder_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0])
        

    radar_chart = pygal.Radar(fill=True, style=DefaultStyle)
    radar_chart.title = 'Player Performance'
    radar_chart.x_labels = ['Batting', 'Bowling', 'Fielding']
    radar_chart.y_labels = [0,20,40,60,80,100]
    radar_chart.add(player, [bat,bowl,field])
    radar_chart.add('Average', [bat_avg,bowl_avg,field_avg])
    return radar_chart.render_data_uri()
