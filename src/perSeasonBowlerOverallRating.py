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
               
bowler_info = data_player.select(data_player.match_id,data_player.bowler,data_player.ball,data_player.wide_runs, data_player.noball_runs, data_player.extra_runs, data_player.total_runs, data_player.player_dismissed, data_player.dismissal_kind)
toIntfunc = udf(lambda x: int(x),IntegerType())
bowler_info2 = bowler_info.withColumn("match_id",toIntfunc(bowler_info['match_id']))

cond1 = func.col("match_id") >= minval
cond2 = func.col("match_id") <= maxval

bowler_info2 = bowler_info2.filter(cond1 & cond2)
bowler_name = bowler_info2.select(bowler_info2.bowler).distinct()

bo_name = [i.bowler for i in bowler_name.collect()]
bowler =''
run0=run1=run2=run3=run5=run4=run6=overall=wickets_player=0
overall_rating = pd.DataFrame({"bowler" : bowler,"run0" : [run0],"run1" : [run1],"run2" : [run2],"run3" : [run3],"run4" : [run4],"run5" : [run5],"run6" : [run6],"wickets" : [wickets_player],"overall" : [overall]})
def cal_bowler_score(bowler):
    bowl_name = bowler_info2.filter(bowler_info.bowler == bowler)
    run0 = bowl_name.filter("total_runs == '0'").count()
    run1 = bowl_name.filter("total_runs == '1'").count()
    run2 = bowl_name.filter("total_runs == '2'").count()
    run3 = bowl_name.filter("total_runs == '3'").count()
    run5 = bowl_name.filter("total_runs == '5'").count()
    run4 = bowl_name.filter("total_runs == '4'").count()
    run6 = bowl_name.filter("total_runs == '6'").count()
    wickets_player = bowler_info2.filter("player_dismissed != 'null'").filter("dismissal_kind != 'run out'").filter(bowler_info.bowler == bowler).count()
    
    overall = 500 + run0 + wickets_player*100 -((run1)*1 + (run2)*2 + (run3)*3 + (run5)*5 + (run4)*4 + (run6)*6)
    
    overall_rating.loc[-1] = [bowler,overall,run0,run1,run2,run3,run4,run5,run6,wickets_player]
    overall_rating.index = overall_rating.index + 1
    overall_rating.sort_index()

for i in bo_name:
    cal_bowler_score(i)
    
overall_rating.to_csv("bowler2008.csv", index=True) #change the output file name for csv of different season
overall_rating.head()