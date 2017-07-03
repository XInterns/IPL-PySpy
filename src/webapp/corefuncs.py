from src.webapp.pyspyInit import *
from src.webapp.jsonifyDF import *
from pyspark.sql.types import *         # for defining schema with various datatypes
import pyspark.sql.functions as func    # for ETL, data processing on Dataframes
from pyspark.sql.functions import udf

import ast                              # for evaluating and converting to a dict
from datetime import *                  # for datetime datatype for schema
from dateutil.parser import parse       # for string parse to date
from bokeh.palettes import *                                       # brewer color palette

########### Common Modules ###########
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
        
    attr_range = attrDF.rdd.map(lambda x: dtype_cast(x[0],dtype)).collect()
    return attr_range

########### Overall Standings Module ###########
def get_overall_ranks_df(season_num, srcDF):
    overall_ranking = srcDF.filter(srcDF.season == season_num)\
                    .groupBy("winner").count().orderBy("count",ascending=0) # extracting required columns into another DF
        
    overall_ranking = overall_ranking.filter("winner != '' ")                          # Deleting records of tied matches
    overall_ranking = overall_ranking.selectExpr("winner as Team", "count as Wins")   # Renaming columns
    return overall_ranking


def overall_rank_jsonify(srcDF, season_num):
    result_DF = get_overall_ranks_df(season_num, srcDF)
    return toJsonObj(result_DF)



########### Performance Consistency Module ###########
def get_consistency_DF(srcDF, season_lbound, season_ubound):    
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
        
    consistency_df = consistency_df.selectExpr("winner as Teams", "final_deviations as Consistency")    
    return consistency_df


def consistency_jsonify(srcDF, season_lbound = 2008, season_ubound = 2016):
    result_DF = get_consistency_DF(srcDF, season_lbound, season_ubound)      # extracting required columns 
    constraints_df = get_constraints(srcDF)
    constraints_list = [i.winner for i in constraints_df.collect()]          # storing a list of filtered teams
    result_DF = filter_using_constraints(result_DF, constraints_list)
    result_DF = calc_consistency(result_DF)

    return toJsonObj(result_DF)



########### Team Vs Team Win Percentage Module ###########
def get_winnerDF(srcDF):
    return srcDF.select(srcDF.team1,srcDF.team2,srcDF.winner)


def team_vs_team_jsonify(srcDF, team1, team2):
    if team1 == team2:                          #check whether the two teams selected are same or not.
        return "SAME TEAM"
    else:
        winnerDF = get_winnerDF(srcDF)

        team1_= winnerDF.filter(winnerDF.team1 == team1)
        team11_ = winnerDF.filter(winnerDF.team2 == team1)
        team2_= team1_.filter(team1_.team2 == team2)
        team22_ = team11_.filter(team11_.team1 == team2)
        
        winners1_ = team2_.filter(team2_.winner == team1)#checking the matches won by the team1
        winners11_ = team22_.filter(team22_.winner == team1)
        winners2_ = team2_.filter(team2_.winner == team2)  #checking the matches won by the team2
        winners22_ = team22_.filter(team22_.winner == team2)
        
        
        #number of matches won by first team
        team1_win = winners1_.count() 
        team1_win2 = winners11_.count()
         
        #number of matches won by second team
        team2_win = winners2_.count()
        team2_win2 = winners22_.count()        
        
        total_matches = team22_.count() + team2_.count() #taking the count of total number of matches
        
        if team1_win+team2_win+team1_win2+team2_win2 != total_matches:    #checking for any matches without any result
            total_matches = total_matches - (total_matches -(team1_win + team2_win + team1_win2 + team2_win2))  #calculating new total matches played with significant result
        
        if total_matches == 0:  #checking if the teams ever played a match between each other
            return "NO MATCHES PLAYED BEFORE" 
        else:
            team1_percent = ((team1_win + team1_win2) * 100)/float(total_matches) #calculating the percentage win for first team
            team2_percent = ((team2_win + team2_win2) * 100)/float(total_matches) #calculating the percentage win for second team
            return jsonify_Percents(team1, team2, team1_percent, team2_percent)



########### Player Performance Module ###########
def get_fieldDF():
    field = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath + "fielder.csv"))
    return field


def get_batDF():
    bat = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath + "batsman.csv"))
    return bat


def get_bowlDF():
    bowl = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(data_opath + "bowler.csv"))
    return bowl


def Player_Performance_jsonify(player):

    batc = get_batDF()
    bowlc = get_bowlDF()
    fieldc = get_fieldDF()

    field_max_overall = int(fieldc.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])

    #droping the duplicate rows having fielder, overall and ratings common.

    field2 = fieldc.dropDuplicates(['fielder','overall','ratings'])

    #calcuating the overall ratings of every fielder relative to the maximum

    fielder_ratings = field2.withColumn('ratings_overall', (fieldc.overall / field_max_overall*100)).sort("overall",ascending=0)

    #toIntfunc is a function which converts the values to integer.

    toIntfunc = udf(lambda x: int(x),IntegerType())

    #converting the column "overall" to integer.

    bat2 = batc.withColumn("overall",toIntfunc(batc['overall']))

    #computing the batsman's overall maximum

    bat_max_overall = int (bat2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])

    #calcuating the overall ratings of every batsman relative to the maximum

    batsman_ratings = bat2.withColumn('ratings_overall', (bat2.overall / bat_max_overall*100)).sort("overall",ascending=0)

    #converting the column "overall" to integer.

    bowl2 = bowlc.withColumn("overall",toIntfunc(bowlc['overall']))

    #computing the bowler's overall maximum

    bowl_max_overall = int (bowl2.describe(['overall']).filter("summary == 'max'").select('overall').collect()[0][0])

    #calcuating the overall ratings of every bowler relative to the maximum

    bowler_ratings = bowl2.withColumn('ratings_overall', (bowl2.overall / bowl_max_overall*100)).sort("overall",ascending=0)

    #converting the new overall ratings to integer.

    bowler_ratings2 = bowler_ratings.withColumn("ratings_overall",toIntfunc(bowler_ratings['ratings_overall']))
    fielder_ratings2 = fielder_ratings.withColumn("ratings_overall",toIntfunc(fielder_ratings['ratings_overall']))
    batsman_ratings2 = batsman_ratings.withColumn("ratings_overall",toIntfunc(batsman_ratings['ratings_overall']))


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
    
    return jsonify_Ratings(player,bat,bowl,field)

