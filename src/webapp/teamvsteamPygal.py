import pyspark 
from pyspark import SparkContext
from pyspark.sql import SQLContext
import matplotlib.pyplot as plt
from io import BytesIO
import base64
from flask import Flask, render_template, request
import pygal    
from pygal.style import *



app = Flask(__name__)

sc = SparkContext()

sql = SQLContext(sc)
data_matches = (sql.read.format("com.databricks.spark.csv").option("header","true").load("/home/rajan/ipl/IPL-PySpy/input/csv/matches.csv"))

team_winner = data_matches.select(data_matches.team1,data_matches.team2,data_matches.winner)


def plotit(first_team,second_team):

	if first_team != second_team:
		team1_= team_winner.filter(team_winner.team1 == first_team)
		team11_ = team_winner.filter(team_winner.team2 == first_team)
		team2_= team1_.filter(team1_.team2 == second_team)
		team22_ = team11_.filter(team11_.team1 == second_team)

		winners1_ = team2_.filter(team2_.winner == first_team)#checking the matches won by the first_team
		winners11_ = team22_.filter(team22_.winner == first_team)
		winners2_ = team2_.filter(team2_.winner == second_team)  #checking the matches won by the second_team
		winners22_ = team22_.filter(team22_.winner == second_team)


		#number of matches won by first team
		first_team_win = winners1_.count()
		first_team_win2 = winners11_.count()
		
		#number of matches won by second team
		second_team_win = winners2_.count()
		second_team_win2 = winners22_.count()        


		total_matches = team22_.count() + team2_.count() #taking the count of total number of matches

		if first_team_win+second_team_win+first_team_win2+second_team_win2 != total_matches:    #checking for any matches without any result
			total_matches = total_matches - (total_matches -(first_team_win + second_team_win + first_team_win2 + second_team_win2))  #calculating new total matches played with significant result

		if total_matches != 0:  #checking if the teams ever played a match between each other
			first_team_percent = ((first_team_win + first_team_win2) * 100)/float(total_matches) #calculating the percentage win for first team
			second_team_percent = ((second_team_win + second_team_win2) * 100)/float(total_matches) #calculating the percentage win for second team
			pie_chart = pygal.Pie(style=LightGreenStyle)
			pie_chart.title = 'Team Vs Team Win Prediction(in %)'
			pie_chart.add(first_team,round(first_team_percent,2))
			pie_chart.add(second_team,round(second_team_percent,2))
			return pie_chart.render_data_uri()

teams = ['Rajasthan Royals','Chennai Super Kings','Deccan Chargers','Gujarat Lions','Delhi Daredevils','Mumbai Indians','Kochi Tuskers Kerala','Royal Challengers Bangalore','Pune Warriors','Rising Pune Supergiants','Sunrisers Hyderabad','Kolkata Knight Riders','Kings XI Punjab']

@app.route('/')
def index():
	# Determine the selected feature
	flag=0
	flag2=0
	result=None
	team1= request.args.get("team1")
	if team1 == None:
		team1 = "Rajasthan Royals"

	team2= request.args.get("team2")
	if team2 == None:
		team2 = "Delhi Daredevils"

	if(team1==team2):
		flag=1
	else:
		result = plotit(team1,team2)
		if result == None:
			flag2=1
		
	return render_template("teamPygal.html",result=result,team1=team1,team2=team2,teams=teams,flag=flag,flag2=flag2)


if __name__ == '__main__':
	app.run(port=5000, debug=True)