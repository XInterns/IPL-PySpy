from flask import Flask, render_template, request
from src.webapp.corefuncs import *

import pygal    
from pygal.style import DefaultStyle, LightGreenStyle

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
        
    attrRange = [str(i[0]) for i in attrDF.collect()]
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
            dates = get_range(srcDF, seasonNum,"date",0,0),
            venues = get_clean_range(get_range(srcDF, seasonNum,"venue",0,0), 0),
            cities = get_range(srcDF, seasonNum,"city",0,0),
            team1 = get_range(srcDF, seasonNum,"team1",0,0),
            team2 = get_range(srcDF, seasonNum,"team2",0,0),
            winners = get_range(srcDF, seasonNum,"winner",0,0),
            player_of_match = get_range\
            (srcDF, seasonNum,"player_of_match",0,0),
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

    # resultpDF = resultDF.toPandas()
    # clr = get_color_list("RdYlGn", resultDF.count())

    # figurePerformanceConsistency = Bar(resultpDF,\
    #         values="Consistency",\
    #         color="Teams", palette=clr,\
    #         label=cat(columns="Teams", sort=False),\
    #         xlabel="Teams", ylabel="Win Consistency %age",\
    #         title="IPL Performance Consistencies "\
    #         +str(season_lbound)+"-"+str(season_ubound),
    #         legend='top_right', plot_width=950, bar_width=0.6)

    # figurePerformanceConsistency.y_range = Range1d(60,100)
    # return figurePerformanceConsistency
    gauge = pygal.SolidGauge(inner_radius=0.70)
    percent_formatter = lambda x: '{:.10g}%'.format(x)
    gauge.value_formatter = percent_formatter
    for row in resultDF.collect():
        gauge.add(str(row[0]), [{'value': round(row[1],2), 'max_value': 100}])
    return gauge.render_data_uri()


########### PlayerPerformance Module ###########
class PlayerPerformance(object):
    """PlayerPerformance module class"""
    def __init__(self):
        self.toIntfunc = udf(lambda x: int(x),IntegerType())                                                                         #converting the column "overall" to integer.
        self.fieldDF = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"fielder.csv"))          #computing the fielder's overall maximum
        self.fieldDF = self.fieldDF.withColumn("overall", self.toIntfunc("overall"))
        self.field_max_overall = int(self.fieldDF.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])      #droping the duplicate rows having fielder, overall and ratings common.
        self.field2 = self.fieldDF.dropDuplicates(['fielder','overall','ratings'])                                                          #calcuating the overall ratings of every fielder relative to the maximum
        self.fielder_ratings = self.field2.withColumn('ratings_overall', ((self.fieldDF.overall*100)/self.field_max_overall)).sort("overall",ascending=0) #reading the batsman.csv file
        self.bat = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"batsman.csv"))            #toIntfunc is a function which converts the values to integer.
        self.bat2 = self.bat.withColumn("overall",self.toIntfunc(self.bat['overall']))                                                              #computing the batsman's overall maximu
        self.bat_max_overall = int (self.bat2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])         #calcuating the overall ratings of every batsman relative to the maximum
        self.batsman_ratings = self.bat2.withColumn('ratings_overall', ((self.bat2.overall*100) / self.bat_max_overall)).sort("overall",ascending=0)  #reading the bowler.csv file
        self.bowl = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath+"bowler.csv"))            #converting the column "overall" to integer.
        self.bowl2 = self.bowl.withColumn("overall",self.toIntfunc(self.bowl['overall']))                                                           #computing the bowler's overall maximum
        self.bowl_max_overall = int (self.bowl2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])       #calcuating the overall ratings of every bowler relative to the maximum
        self.bowler_ratings = self.bowl2.withColumn('ratings_overall', ((self.bowl2.overall*100) / self.bowl_max_overall)).sort("overall",ascending=0)#converting the new overall ratings to integer.
        self.bowler_ratings2 = self.bowler_ratings.withColumn("ratings_overall",self.toIntfunc(self.bowler_ratings['ratings_overall']))
        self.fielder_ratings2 = self.fielder_ratings.withColumn("ratings_overall",self.toIntfunc(self.fielder_ratings['ratings_overall']))
        self.batsman_ratings2 = self.batsman_ratings.withColumn("ratings_overall",self.toIntfunc(self.batsman_ratings['ratings_overall']))
        self.bat_avg = round(self.batsman_ratings2.agg(func.avg(func.col('ratings_overall'))).collect()[0][0],2)
        self.bowl_avg = round(self.bowler_ratings2.agg(func.avg(func.col('ratings_overall'))).collect()[0][0],2)
        self.field_avg = round(self.fielder_ratings2.agg(func.avg(func.col('ratings_overall'))).collect()[0][0],2)
        self.baseBatScore = 5
        self.baseBowlScore = 2
        self.baseFieldScore = 5

    def getPlayerNames(self):
        playerList = [str(i[1]) for i in self.fielder_ratings2.collect()]
        # playerList.remove("None")
        playerList.sort()
        playerList=["Average"]+playerList
        return playerList

    def get_player_batting_rating(self, player):
        if(player=="Average"):
            return self.bat_avg
        batScoreList = self.batsman_ratings2.filter(self.batsman_ratings2.batsman==player).collect()
        if(len(batScoreList)):
            batScore = int(batScoreList[0][9])
        else:
            batScore = self.baseBatScore
        return batScore
        
    def get_player_bowling_rating(self, player):
        if(player=="Average"):
            return self.bowl_avg
        bowlScoreList = self.bowler_ratings2.filter(self.bowler_ratings2.bowler==player).collect()
        if(len(bowlScoreList)):
            bowlScore = int(bowlScoreList[0][11])
        else:
            bowlScore = self.baseBowlScore
        return bowlScore

    def get_player_fielding_rating(self, player):       
        if(player=="Average"):
            return self.field_avg
        fieldScoreList = self.fielder_ratings2.filter(self.fielder_ratings2.fielder==player).collect()
        if(len(fieldScoreList)):
            fieldScore = int(fieldScoreList[0][4])
        else:
            fieldScore = self.baseFieldScore
        return fieldScore

    def create_figure_player_performance(self, player1, player2):
        radar_chart = pygal.Radar(fill=True, style=DefaultStyle)
        radar_chart.title = 'Player Performance'
        radar_chart.x_labels = ['Batting', 'Bowling', 'Fielding']
        radar_chart.y_labels = [0,20,40,60,80,100]
        radar_chart.add(player1, [\
            self.get_player_batting_rating(player1),\
            self.get_player_bowling_rating(player1),\
            self.get_player_fielding_rating(player1)])
        radar_chart.add(player2, [\
            self.get_player_batting_rating(player2),\
            self.get_player_bowling_rating(player2),\
            self.get_player_fielding_rating(player2)])
        return radar_chart.render_data_uri()

########### Team Vs Team Win Percentage Module ###########
def create_figure_team_vs_team_win_percentage(srcDF, first_team,second_team):
    team_winner = srcDF.select(srcDF.team1,srcDF.team2,srcDF.winner)
    if first_team != second_team:
        team1_= team_winner.filter(team_winner.team1 == first_team)
        team11_ = team_winner.filter(team_winner.team2 == first_team)
        team2_= team1_.filter(team1_.team2 == second_team)
        team22_ = team11_.filter(team11_.team1 == second_team)

        winners1_ = team2_.filter(team2_.winner == first_team)#checking the matches won by the first_team
        winners11_ = team22_.filter(team22_.winner == first_team)
        winners2_ = team2_.filter(team2_.winner == second_team)  #checking the matches won by the second_team
        winners22_ = team22_.filter(team22_.winner == second_team)


        #number of matches won by first team
        first_team_win = winners1_.count()
        first_team_win2 = winners11_.count()
        
        #number of matches won by second team
        second_team_win = winners2_.count()
        second_team_win2 = winners22_.count()        


        total_matches = team22_.count() + team2_.count() #taking the count of total number of matches

        if first_team_win+second_team_win+first_team_win2+second_team_win2 != total_matches:    #checking for any matches without any result
            total_matches = total_matches - (total_matches -(first_team_win + second_team_win + first_team_win2 + second_team_win2))  #calculating new total matches played with significant result

        if total_matches != 0:  #checking if the teams ever played a match between each other
            first_team_percent = ((first_team_win + first_team_win2) * 100)/float(total_matches) #calculating the percentage win for first team
            second_team_percent = ((second_team_win + second_team_win2) * 100)/float(total_matches) #calculating the percentage win for second team
            pie_chart = pygal.Pie(style=LightGreenStyle)
            percent_formatter = lambda x: '{:.10g}%'.format(x)
            pie_chart.value_formatter = percent_formatter
            pie_chart.title = 'Team Vs Team Win Prediction(in %)'
            pie_chart.add(first_team,round(first_team_percent,2))
            pie_chart.add(second_team,round(second_team_percent,2))
            return pie_chart.render_data_uri()


########### Dream Team Module ###########
def create_team(season1,season2):
    teamDF = (sql.read.format("com.databricks.spark.csv").\
            option("header","true").load(data_opath + "dreamTeam" + season1+"_"+season2+".csv"))
    yaxis = [str(x) for x in range(1, 7)]
    xaxis = ["a","b"]
    player=[str(i.name) for i in teamDF.collect()]
    batsman_overall=[int(i.batsmanrating) for i in teamDF.collect()]
    bowler_overall=[int(i.bowlerrating) for i in teamDF.collect()]
    fielder_overall=[int(i.fielderrating) for i in teamDF.collect()]
    strikerate=[float(i.strikerate) for i in teamDF.collect()]
    economyrate=[float(i.economyrate) for i in teamDF.collect()]
    ycoordinate =["Batsman","Batsman","Batsman","Batsman","Batsman",\
        "Batsman","Bowler","Bowler","Bowler","Bowler","Bowler"]

    colormap = {
        
        "Batsman"      : "#0092CD",
        "Bowler"       : "#4FC730",
        
    }

    source = ColumnDataSource(
        data=dict(
            role = [i for i in ycoordinate],
            xpoints = ["a","a","a","a","a","a","b","b","b","b","b"],
            ypoints = [2,3,4,5,6,1,6,5,4,3,2],
            player = [i for i in player],
            batscore = [i for i in batsman_overall],
            bowlscore = [i if i != 0 else 2 for i in bowler_overall],
            fieldscore = [i for i in fielder_overall],
            srate = [i if i!= 100.0 else "NA" for i in strikerate],
            erate = [i if i!= 100.0 else "NA" for i in economyrate],
            type_color = [colormap[x] for x in ycoordinate],
        )
    )

    p = figure(title = "Dream Team", x_range = xaxis, y_range = list(reversed(yaxis)), tools ="hover")
    p.plot_width = 800
    p.plot_height = 500
    p.toolbar_location = None
    p.outline_line_color = None
    p.axis.visible = False

    rect = Rect(x="xpoints", y="ypoints", width=0.9, height=0.9, fill_color="type_color")
    rect_render = p.add_glyph(source, rect)

    legend = Legend(items=[LegendItem(label=field("role"), renderers=[rect_render])])
    p.add_layout(legend, 'right')

    text_props = {
        "source": source,
        "angle": 0,
        "color": "black",
        "text_align": "center",
        "text_baseline": "middle",
    }

    p.text(x="xpoints", y="ypoints", text="player",
           text_font_style="bold", text_font_size="12pt", **text_props)

    p.grid.grid_line_color = None

    p.select_one(HoverTool).tooltips = [
        ("player","@player"),
        ("role","@role"),
        ("batting","@batscore"),
        ("bowling","@bowlscore"),
        ("fielding","@fieldscore"),
        ("strikerate","@srate"),
        ("economyrate","@erate")
    ]
    return p
