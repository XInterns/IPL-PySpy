from jsonifyDF import *
from flask import Flask, jsonify, request

app = Flask(__name__)
mdf = getMatchDF()

@app.route("/", methods=["GET"])
def test():
	return jsonify({"message":"It works!"})

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
	print lbound
	print ubound
	return jsonify({"Performance_Consistency_"+str(lbound)+"_to_"+str(ubound): consistency_jsonify(mdf, lbound, ubound)})

if __name__ == "__main__":
	app.run(port=5000)
	