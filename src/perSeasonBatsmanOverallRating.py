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

data_player = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_path + "deliveries.csv"))

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
               
batsman_info = data_player.select(data_player.match_id,data_player.batsman,data_player.batsman_runs,data_player.player_dismissed)
toIntfunc = udf(lambda x: int(x),IntegerType())
batsman_info2 = batsman_info.withColumn("match_id",toIntfunc(batsman_info['match_id']))

cond1 = func.col("match_id") >= minval
cond2 = func.col("match_id") <= maxval

batsman_info2 = batsman_info2.filter(cond1 & cond2)
player = batsman_info2.groupBy(batsman_info2.batsman,batsman_info2.batsman_runs).count().orderBy("batsman","batsman_runs")


batsman_name = batsman_info2.select(batsman_info2.batsman).distinct()

batsman = ''
run0=run1=run2=run3=run4=run6=overall=0
b_name = [i.batsman for i in batsman_name.collect()]
overall_rating = pd.DataFrame({"batsman" : batsman,"run0" : [run0],"run1" : [run1],"run2" : [run2],"run3" : [run3],"run4" : [run4],"run6" : [run6],"overall" : [overall]})
def cal_batsman_score(batsman):
    bat_name = batsman_info2.filter(batsman_info2.batsman == batsman)
    run0 = bat_name.filter("batsman_runs == '0'").count()
    run1 = bat_name.filter("batsman_runs == '1'").count()
    run2 = bat_name.filter("batsman_runs == '2'").count()
    run3 = bat_name.filter("batsman_runs == '3'").count()
    run4 = bat_name.filter("batsman_runs == '4'").count()
    run6 = bat_name.filter("batsman_runs == '6'").count()
    overall = (50 + run1 + run2 + run3 + (run4)*8 + (run6)*12 - run0)


    overall_rating.loc[-1] = [batsman,overall,run0,run1,run2,run3,run4,run6]
    overall_rating.index = overall_rating.index + 1
    overall_rating.sort_index()

for i in b_name:
    cal_batsman_score(i)
    

overall_rating.to_csv("batsman2008.csv", index=True) #change the output file name for csv of different season
overall_rating.head()