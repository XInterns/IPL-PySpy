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
	return jsonify({"Overall_Standings_"+season: overall_rank_jsonify(season,mdf)})

if __name__ == "__main__":
	app.run(port=5000)
	