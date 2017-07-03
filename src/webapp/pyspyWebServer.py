from src.webapp.sparkInit import sc, sql
from src.webapp.jsonifyDF import *
from flask import Flask, jsonify, request

app = Flask(__name__)
mdf = getMatchDF()

@app.route("/", methods=["GET"])
def test():
	return ''' <h1>Welcome!</h1></br>
	<b>Modules directory:</b></br>
	<a href="/overallstandings/help">Team Overall Standings</a></br>
	<a href="/consistency/help">Team Performance Consistency</a></br>
	<a href="/TeamVsTeamWinPercentage/help">Team V/S Team Win Percentage</a></br>
	<a href="/PlayerPerformance/help">Player Performance</a></br>'''


@app.route("/overallstandings/help", methods=["GET"])
def returnOverallStandingsHelp():
	return ''' <h1>**** Team Overall Standings ****</h1></br>
	<b>about:</b> returns json data of overall standings for each team on the basis of total wins</br>
	<b>usage:</b> /overallstandings?season=2013</br>
	<b>range:</b> 2008 - 2016</br>'''

@app.route("/consistency/help", methods=["GET"])
def returnPerformanceConsistenciesHelp():
	return ''' <h1>**** Team Performance Consistency ****</h1></br>
	<b>about:</b> returns json data of performance consistencies for each team</br>
	<b>usage:</b> /consistency?lbound=2009&ubound=2012</br>
	<b>range:</b> 2008-2016</br>'''

@app.route("/TeamVsTeamWinPercentage/help", methods=["GET"])
def returnTeamVsTeamWinPercentageHelp():
	return ''' <h1>**** Team V/S Team Win Percentage ****</h1></br>
	<b>about:</b> returns json data of win percentages of given teams against each other</br>
	<b>usage:</b> /TeamVsTeamWinPercentage?team1=Chennai+Super+Kings&team2=Delhi+Daredevils</br>
	<b>teams:</b></br>
	<ul>
		<li>Rajasthan Royals</li>
		<li>Chennai Super Kings</li>
		<li>Deccan Chargers</li>
		<li>Gujarat Lions</li>
		<li>Delhi Daredevils</li>
		<li>Mumbai Indians</li>
		<li>Kochi Tuskers Kerala</li>
		<li>Royal Challengers Bangalore</li>
		<li>Pune Warriors</li>
		<li>Rising Pune Supergiants</li>
		<li>Sunrisers Hyderabad</li>
		<li>Kolkata Knight Riders</li>
		<li>Kings XI Punjab</li>
	</ul>'''

@app.route("/PlayerPerformance/help", methods=["GET"])
def returnPlayerPerformanceHelp():
	return ''' <h1>**** Player Performance ****</h1></br>
	<b>about:</b> returns json data of overall performance for each player</br>
	<b>usage:</b> /PlayerPerformance?player=V+Kohli</br>'''


@app.route("/overallstandings", methods=["GET"])
def returnOverallStandings():
	args = request.args
	season = args['season']
	return jsonify({"Overall_Standings_"+season: overall_rank_jsonify(mdf, season)})

@app.route("/consistency", methods=["GET"])
def returnPerformanceConsistencies():
	args = request.args
	lbound = int(args['lbound'])
	ubound = int(args['ubound'])
	return jsonify({"Performance_Consistency_"+str(lbound)+"_to_"+str(ubound): consistency_jsonify(mdf, lbound, ubound)})

@app.route("/TeamVsTeamWinPercentage", methods=["GET"])
def returnTeamVsTeamWinPercentage():
	args = request.args
	team1 = args['team1']
	team2 = args['team2']
	return jsonify({"Team_Vs_Team_Win_Percentage_"+team1+"_VS_"+team2: team_vs_team_jsonify(mdf, team1, team2)})

@app.route("/PlayerPerformance", methods=["GET"])
def returnPlayerPerformance():
	args = request.args
	player = args['player']
	return jsonify({"Player_Performance_"+player: Player_Performance_jsonify(player)})

if __name__ == "__main__":
	app.run(port=5000)
