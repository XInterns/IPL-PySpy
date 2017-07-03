from pyspark.sql.types import *         # for defining schema with various datatypes
import pyspark.sql.functions as func    # for ETL, data processing on Dataframes
from pyspark.sql.functions import udf

import ast                              # for evaluating and converting to a dict
from datetime import *                  # for datetime datatype for schema
from dateutil.parser import parse       # for string parse to date

########### Common Modules ###########
def toJsonObj(srcDF):
    json_obj = []
    srcDF_size = srcDF.count()
    srcDF_list = srcDF.toJSON().collect()
    
    for i in range(srcDF_size):
        obj = srcDF_list[i]
        json_obj.append(dict(ast.literal_eval(obj)))
    return json_obj


def jsonify_Percents(team1, team2, team1_percent, team2_percent):
    json_obj = []
    
    entry1 = {}
    entry1['team1']=team1
    entry1['win_percent']=team1_percent

    entry2 = {}
    entry2['team1']=team2
    entry2['win_percent']=team2_percent

    json_obj.append(entry1)
    json_obj.append(entry2)
    return json_obj


def jsonify_Ratings(player,bat,bowl,field):
    json_obj = []
    
    entry = {}
    entry['Player']=player
    entry['batting']=bat
    entry['bowling']=bowl
    entry['fielding']=field

    json_obj.append(entry)
    return json_obj
