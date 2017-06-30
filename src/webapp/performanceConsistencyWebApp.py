from flask import Flask, render_template, request
from bokeh.embed import components

import pyspark                      
from pyspark import SparkContext 
from pyspark.sql import SQLContext
import pyspark.sql.functions as func    # for ETL, data processing on Dataframes

import pandas as pd                     # converting PysparkDF to PandasDF when passing it as a parameter to Bokeh invokes 

from datetime import *                  # for datetime datatype for schema
from dateutil.parser import parse       # for string parse to date

from bokeh.charts import Bar, output_file, show            # creating bar charts, and displaying it
from bokeh.charts.attributes import cat                    # extracting column for 'label' category in bar charts
from bokeh.io import output_file, show
from bokeh.models import Range1d                               # brewer color palette
from bokeh.palettes import *
from bokeh.plotting import figure, show, output_file,curdoc

app = Flask(__name__)
sc = SparkContext()
sql = SQLContext(sc)

data_path = "input/csv/"                                # path directory to input csv files
data_opath = "output/csv/"                              # path directory to output csv files


########### Common Module ###########
def getMatchDF():
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

def dTypeCast(data, dtype):
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


def getDropDownList(srcDF, attr, sort_req, dtype):
    attrDF = srcDF.select(attr).distinct()
    
    if sort_req:
        attrDF = attrDF.orderBy(attr)
        
    attr_range = attrDF.rdd.map(lambda x: dTypeCast(x[0],dtype)).collect()
    return attr_range

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

    resultpDF = resultDF.toPandas()
    clr = get_color_list("RdYlGn", resultDF.count())

    figurePerformanceConsistency = Bar(resultpDF,\
            values="Consistency",\
            color="Teams", palette=clr,\
            label=cat(columns="Teams", sort=False),\
            xlabel="Teams", ylabel="Win Consistency %age",\
            title="IPL Performance Consistencies "\
            +str(season_lbound)+"-"+str(season_ubound),
            legend='top_right', plot_width=950, bar_width=0.6)

    figurePerformanceConsistency.y_range = Range1d(60,100)
    return figurePerformanceConsistency

mdf = getMatchDF()
lboundList = getDropDownList(mdf,"season",1,"int")
uboundList = getDropDownList(mdf,"season",1,"int")

# Index page
@app.route('/')
def index():
    # Determine the selected feature
    lbound = request.args.get("lbound")
    ubound = request.args.get("ubound")
    print "here"
    
    if lbound == None:
       lbound = 2009
    else:
       lbound = int(lbound)

    if ubound == None:
        ubound = 2012
    else:
        ubound = int(ubound)

    if(lbound > ubound):
        lbound^=ubound
        ubound^=lbound
        lbound^=ubound
    # Create the plot
    plot = create_figure_performance_consistency(mdf, lbound, ubound)
	# Embed plot into HTML via Flask Render
    script, div = components(plot)
    return render_template("performanceConsistency.html",\
            script=script, div=div, lboundList=lboundList,\
            uboundList=uboundList, lbound=lbound,\
            ubound=ubound)

# With debug=True, Flask server will auto-reload 
# when there are code changes
if __name__ == '__main__':
	app.run(port=5000, debug=True)
