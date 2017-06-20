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
data_opath = "../output/json/"                               # path directory to output csv files

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
    
def overall_rank_jsonify(season_num, srcDF):
    srcDF = srcDF.filter(srcDF.season == season_num)\
                    .groupBy("winner").count().orderBy("count",ascending=0) # extracting required columns into another DF
    srcDF = srcDF.filter("winner != '' ")                          # Deleting records of tied matches
    srcDF = srcDF.selectExpr("winner as Team", "count as Wins")   # Renaming columns
    
    return toJsonObj(srcDF)
