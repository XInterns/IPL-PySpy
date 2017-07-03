from src.webapp.corefuncs import *
from flask import Flask, render_template, request

from bokeh.charts import Bar            # creating bar charts, and displaying it
from bokeh.charts.attributes import cat                    # extracting column for 'label' category in bar charts
from bokeh.embed import components
from bokeh.palettes import *                               # brewer color palette

app = Flask(__name__)

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


########### Overall Standings Module ###########
def get_overall_ranks_df(season_num, srcDF):
    overallRanking = srcDF.filter(srcDF.season == season_num)\
                    .groupBy("winner").count().orderBy("count",ascending=0) # extracting required columns into another DF
    
    overallRanking = overallRanking.filter("winner != '' ")                          # Deleting records of tied matches
    overallRanking = overallRanking.selectExpr("winner as Team", "count as Wins")   # Renaming columns
    return overallRanking


def create_figure_overall_ranks(srcDF, season_num):
    overallRanksDF = get_overall_ranks_df(season_num, srcDF)
    overallRankspDF = overallRanksDF.toPandas()
    clr = get_color_list('Viridis', overallRanksDF.count())  # Brewing color hex values for each tuple('team')
    figureOverallRanks = Bar(overallRankspDF, values="Wins", color="Team",\
                    palette=clr, label=cat(columns="Team", sort=False),\
                    xgrid=True, xlabel="Team", ylabel="Wins",\
                    title="Overall Standings " + str(season_num),\
                    legend='top_right', plot_width=950, bar_width=0.6)   # generating bar chart
    return figureOverallRanks

mdf = get_match_df()
seasonList = get_dropdown_list(mdf,"season",1,"int")

# Index page
@app.route('/')
def index():
	# Determine the selected feature
	season = request.args.get("season")
	if season == None:
		season = 2013
	else:
		season = int(season)

	# Create the plot
	plot = create_figure_overall_ranks(mdf, season)
		
	# Embed plot into HTML via Flask Render
	script, div = components(plot)
	return render_template("overallRanks.html", script=script, div=div, seasonList=seasonList, season=season)

# With debug=True, Flask server will auto-reload 
# when there are code changes
if __name__ == '__main__':
	app.run(port=5000, debug=True)
