from flask import Flask, render_template, request, Response

import pyspark 
from pyspark import SparkContext
from pyspark.sql import SQLContext

#importing types and functions for converting string type to integer type. udf is an user defined function

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

#importing bokeh for visualization

import pygal    
from pygal.style import *
import pyspark.sql.functions as func

app = Flask(__name__)

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
def avg_bat():
    batsman_avg = batsman_ratings2.agg(func.avg(func.col('ratings_overall')))
    x = batsman_avg.collect()[0][0]
    return round(x,2)

def avg_bowl():
    bowler_avg = bowler_ratings2.agg(func.avg(func.col('ratings_overall')))
    x = bowler_avg.collect()[0][0]
    return round(x,2)

def avg_field():
    fielder_avg = fielder_ratings2.agg(func.avg(func.col('ratings_overall')))
    x = fielder_avg.collect()[0][0]
    return round(x,2)

def create_figure(player):
    
    
    batsman_name = batsman_ratings2.filter(batsman_ratings2.batsman == player)
    bowler_name = bowler_ratings2.filter(bowler_ratings2.bowler == player)
    fielder_name = fielder_ratings2.filter(fielder_ratings2.fielder == player)

    if batsman_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        bat = 10
    else:
        bat = int(batsman_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0])
        
    if bowler_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
        bowl = 5
    else:
        bowl = int(bowler_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0])
        
    if fielder_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0] is None:
    	field = 25
    else:
        field = int(fielder_name.describe(['ratings_overall']).filter("summary == 'max'").select('ratings_overall').collect()[0][0])
        
    
    bat_avg=avg_bat()
    bowl_avg=avg_bowl()
    field_avg=avg_field()

    radar_chart = pygal.Radar(fill=True, style=DefaultStyle)
    radar_chart.title = 'Player Performance'
    radar_chart.x_labels = ['Batting', 'Bowling', 'Fielding']
    radar_chart.y_labels = [0,20,40,60,80,100]
    radar_chart.add(player, [bat,bowl,field])
    radar_chart.add('Average', [bat_avg,bowl_avg,field_avg])
    return radar_chart.render_data_uri()

player_names = fielder_ratings2.rdd.map(lambda x: str(x[1])).collect()

# Index page
@app.route('/')
def index():
	# Determine the selected feature
	player= request.args.get("player")
	if player == None:
		player = "V Kohli"

	# Create the plot
	plot = create_figure(player)
		
	# Embed plot into HTML via Flask Render
	
	return render_template("playerp.html",plot=plot,player_names=player_names,player=player)

# With debug=True, Flask server will auto-reload 
# when there are code changes
if __name__ == '__main__':
	app.run(port=5000, debug=True)

