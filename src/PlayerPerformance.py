'''
Player Performance Chart gives you the clear image of the parameters of batting, bowling, and fielding
for every player who played for the 9 seasons of the IPL.

Taken into account these overall ratings one can see how the players have performed in the previous seasons
and can pick these players for the upcoming seasons.
'''


#importing pyspark, SparkContext, SQLContext for reading the files and making rdd and dataframes.

import pyspark 
from pyspark import SparkContext
from pyspark.sql import SQLContext

#importing types and functions for converting string type to integer type. udf is an user defined function

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

#importing bokeh for visualization

from bokeh.layouts import row
from bokeh.plotting import figure, show, output_file,curdoc
from bokeh.io import output_file, show
from bokeh.layouts import widgetbox
from bokeh.models.widgets import Select


sc = SparkContext()

#reading the fielder.csv file

sql = SQLContext(sc)
field = (sql.read.format("com.databricks.spark.csv").option("header", "true").load("/home/rajan/fielder.csv"))

#computing the fielder's overall maximum

field_max_overall = int (field.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])

#droping the duplicate rows having fielder, overall and ratings common.

field2 = field.dropDuplicates(['fielder','overall','ratings'])

#calcuating the overall ratings of every fielder relative to the maximum

fielder_ratings = field2.withColumn('ratings_overall', (field.overall / field_max_overall*100)).sort("overall",ascending=0)

#reading the batsman.csv file

sql1 = SQLContext(sc)
bat = (sql1.read.format("com.databricks.spark.csv").option("header", "true").load("/home/rajan/batsman.csv"))

#toIntfunc is a function which converts the values to integer.

toIntfunc = udf(lambda x: int(x),IntegerType())

#converting the column "overall" to integer.

bat2 = bat.withColumn("overall",toIntfunc(bat['overall']))

#computing the batsman's overall maximum

bat_max_overall = int (bat2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])

#calcuating the overall ratings of every batsman relative to the maximum

batsman_ratings = bat2.withColumn('ratings_overall', (bat2.overall / bat_max_overall*100)).sort("overall",ascending=0)

#reading the bowler.csv file

sql2 = SQLContext(sc)
bowl = (sql2.read.format("com.databricks.spark.csv").option("header", "true").load("/home/rajan/bowler.csv"))

#converting the column "overall" to integer.

bowl2 = bowl.withColumn("overall",toIntfunc(bowl['overall']))

#computing the bowler's overall maximum

bowl_max_overall = int (bowl2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])

#calcuating the overall ratings of every bowler relative to the maximum

bowler_ratings = bowl2.withColumn('ratings_overall', (bowl2.overall / bowl_max_overall*100)).sort("overall",ascending=0)

#converting the new overall ratings to integer.

bowler_ratings2 = bowler_ratings.withColumn("ratings_overall",toIntfunc(bowler_ratings['ratings_overall']))
fielder_ratings2 = fielder_ratings.withColumn("ratings_overall",toIntfunc(fielder_ratings['ratings_overall']))
batsman_ratings2 = batsman_ratings.withColumn("ratings_overall",toIntfunc(batsman_ratings['ratings_overall']))

'''
create_figure in a method to create the visualization, taking "player" as an argument 
which would be passed through a dropdown select menu.
'''

def create_figure(player):
    
    
    batsman_name = batsman_ratings2.filter(batsman_ratings2.batsman == player)
    bowler_name = bowler_ratings2.filter(bowler_ratings2.bowler == player)
    fielder_name = fielder_ratings2.filter(fielder_ratings2.fielder == player)

    if batsman_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        bat = 10
    else:
        bat = batsman_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0]
        
    if bowler_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        bowl = 5
    else:
        bowl = bowler_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0]
        
    if fielder_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
    	field = 25
    else:
        field = fielder_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0]
        
    
    factors = ["batting","bowling","fielding"]
    x = [bat,bowl,field] 
    dot = figure(title=player, tools="pan,box_zoom,reset",y_range=factors, x_range=[0,100])

    dot.segment(x0=[40], y0=[0], x1=[40],y1=[100], color="#F4A582",line_width=3)
    dot.segment(0, factors, x, factors, line_width=2, line_color="green")
    dot.circle(x, factors, size=15, fill_color="orange", line_color="green", line_width=3)
    return dot


# update is a method for dynamically changing the player name and generating the chart.

def update(attr, old, new):
	layout.children[1] = create_figure(str(new))


#menu is the list of all the players.

menu = fielder_ratings2.rdd.map(lambda x: str(x[1])).collect()

#select is a selection bar with the default value and the options as menu.

select = Select(title="player:", value="V Kohli", options=menu)
select.on_change('value', update)

#displaying the chart and the selection menu bar. 

controls = widgetbox(select, width=200)
layout = row(controls, create_figure(select.value))

curdoc().add_root(layout)

#giving the title for the chart.

curdoc().title = "Player Performance"