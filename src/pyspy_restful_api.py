from jsonifyDF import *
from flask import Flask, jsonify, request

app = Flask(__name__)
mdf = getMatchDF()

@app.route("/", methods=["GET"])
def test():
	return ''' <h1>Welcome!</h1></br>
	<b>Modules directory:</b></br>
	<a href="/overallstandings/help">Team Overall Standings</a></br>
	<a href="/consistency/help">Team Performance Consistency</a></br>'''

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
	<b>usage:</b> /consistency?lbound=2010&ubound=2013</br>
	<b>range:</b> 2008-2016</br>'''

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

if __name__ == "__main__":
	app.run(port=5000)
	