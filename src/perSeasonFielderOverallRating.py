import pyspark 
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import pyspark.sql.functions as func

sc = SparkContext()

sql = SQLContext(sc)

data_player = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_path +"deliveries.csv"))

data_matches = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_path + "matches.csv"))

match_info = data_matches.select(data_matches.id,data_matches.season)

def max_matches_id(season):
    season_1 = match_info.filter(match_info.season == season)
    toIntfunc = udf(lambda x: int(x),IntegerType())
    season_1 = season_1.withColumn("id",toIntfunc(season_1['id']))
    maxval = int (season_1.describe(['id']).filter("summary == 'max'").select('id').collect()[0][0])
    return maxval

def min_matches_id(season):
    season_1 = match_info.filter(match_info.season == season)
    toIntfunc = udf(lambda x: int(x),IntegerType())
    season_1 = season_1.withColumn("id",toIntfunc(season_1['id']))
    minval = int (season_1.describe(['id']).filter("summary == 'min'").select('id').collect()[0][0])
    return minval
    
season = "2008"    # change the season value to get the csv file of that season
minval = min_matches_id(season)
maxval = max_matches_id(season)

bowler_name = data_player.select(data_player.match_id,data_player.bowler,data_player.dismissal_kind,data_player.fielder).distinct()
batsman_name = data_player.select(data_player.match_id,data_player.batsman,data_player.dismissal_kind,data_player.fielder).distinct()

toIntfunc = udf(lambda x: int(x),IntegerType())
bowler_name = bowler_name.withColumn("match_id",toIntfunc(bowler_name['match_id']))

cond1 = func.col("match_id") >= minval
cond2 = func.col("match_id") <= maxval

bowler_name = bowler_name.filter(cond1 & cond2)

batsman_name = batsman_name.withColumn("match_id",toIntfunc(batsman_name['match_id']))
batsman_name= batsman_name.filter(cond1 & cond2)

f_name = [i.bowler for i in bowler_name.collect()]
f2_name = [i.batsman for i in batsman_name.collect()]
fielder =''
ratings=overall=0
overall_rating = pd.DataFrame({"fielder" : fielder,"ratings" : [ratings],"overall" : [overall]})
def cal_fielder_score(fielder):
    
    ratings = batsman_name.filter(batsman_name.fielder == fielder).filter("dismissal_kind != 'null'").count()
    
    overall = 100 + (ratings)*20
    
    overall_rating.loc[-1] = [fielder,overall,ratings]
    overall_rating.index = overall_rating.index + 1
    overall_rating.sort_index()

def cal_fielder_score2(fielder):
    
    ratings = bowler_name.filter(bowler_name.fielder == fielder).filter("dismissal_kind != 'null'").count()
    
    overall = 100 + (ratings)*20
    
    overall_rating.loc[-1] = [fielder,overall,ratings]
    overall_rating.index = overall_rating.index + 1
    overall_rating.sort_index()


for i in f_name:
    cal_fielder_score2(i)

for i in f2_name:
    cal_fielder_score(i)


overall_rating.to_csv("fielder2008.csv", index=True)  #change the output file name for csv of different season
overall_rating.head()