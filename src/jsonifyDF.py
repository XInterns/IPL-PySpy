import pyspark                
from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql.types import *         # for defining schema with various datatypes
import pyspark.sql.functions as func    # for ETL, data processing on Dataframes

import ast                              # for evaluating and converting to a dict
from datetime import *                  # for datetime datatype for schema
from dateutil.parser import parse       # for string parse to date

sc = SparkContext("local","jsonifyApp")
sql = SQLContext(sc)

data_path = "../input/csv/"                                # path directory to input csv files
data_opath = "../output/csv/"                               # path directory to output csv files



########### Common Modules ###########
def getMatchDF():
	match_rdd = sc.textFile(data_path + "matches.csv")         # reading csv files into RDD

	match_header = match_rdd.filter(lambda l: "id,season" in l)     # storing the header tuple
	match_no_header = match_rdd.subtract(match_header)              # subtracting it from RDD
	match_temp_rdd = match_no_header.map(lambda k: k.split(','))\
	.map(lambda p: (int(p[0]), p[1],p[2],parse(p[3]).date(),p[4]\
	                ,p[5],p[6],p[7],p[8],p[9]=='1',p[10],int(p[11])\
	                ,int(p[12]),p[13],p[14],p[15],p[16],p[17]))     # Transforming csv file data

	match_df = sql.createDataFrame(match_temp_rdd, match_rdd.first().split(','))  # converting to PysparkDF
	match_df = match_df.orderBy(match_df.id.asc())                                # asc sort by id
	return match_df

def toJsonObj(srcDF):
    json_obj = []
    srcDF_size = srcDF.count()
    srcDF_list = srcDF.toJSON().collect()
    
    for i in range(srcDF_size):
        obj = srcDF_list[i]
        json_obj.append(dict(ast.literal_eval(obj)))
    return json_obj



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