from flask import Flask, jsonify, request
from src.webapp.webapps import *

app = Flask(__name__)
mdf = get_match_df()


########### DropDown Lists ###########
seasonList = get_dropdown_list(mdf,"season",1,"int")
lboundList = get_dropdown_list(mdf,"season",1,"int")
uboundList = get_dropdown_list(mdf,"season",1,"int")
player_names = get_fielder_ratings2().rdd.map(lambda x: str(x[1])).collect()


########### Index Page ###########
@app.route("/", methods=["GET"])
def index():
    return render_template('index.html')


########### RESTful API Routing & Functionality ###########
@app.route("/overallStandings/help", methods=["GET"])
def returnOverallStandingsHelp():
	return render_template("overallRanksHelp.html")

@app.route("/performanceConsistency/help", methods=["GET"])
def returnPerformanceConsistenciesHelp():
	return render_template("performanceConsistencyHelp.html")


@app.route("/TeamVsTeamWinPercentage/help", methods=["GET"])
def returnTeamVsTeamWinPercentageHelp():
	return render_template("teamVsTeamWinPercentage.html")


@app.route("/PlayerPerformance/help", methods=["GET"])
def returnPlayerPerformanceHelp():
	return render_template("playerperformanceHelp.html")

@app.route("/overallStandings", methods=["GET"])
def returnOverallStandings():
	args = request.args
	season = args['season']
	return jsonify({"Overall_Standings_"+season: overall_rank_jsonify(mdf, season)})


@app.route("/performanceConsistency", methods=["GET"])
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


########### WebApp Routing & Functionality ###########
@app.route("/PlayerPerformance/webapp")
def returnPlayerPerformApp():
    # Determine the selected feature
    player= request.args.get("player")
    if player == None:
        player = "V Kohli"

    # Create the plot
    plot = create_figure_player_performance(player)
        
    # Embed plot into HTML via Flask Render
    script, div = components(plot)
    return render_template("playerperformance.html", script=script, div=div, player_names=player_names, player=player)


@app.route("/seasonOverview/webapp")
def returnSeasonOverviewWebApp():
    # Determine the selected feature
    season = request.args.get("season")
    if season == None:
        season = 2013
    else:
        season = int(season)

    # Create the plot
    plot = create_figure_season_overview(mdf, season)
    
    # Embed plot into HTML via Flask Render
    script, div = components(plot)
    return render_template("seasonOverview.html",script=script,\
            div=div, seasonList=seasonList, season=season)


@app.route("/overallStandings/webapp")
def returnOverallStandingsWebApp():
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


@app.route("/performanceConsistency/webapp")
def returnPerformanceConsistencyWebApp():
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


if __name__ == "__main__":
	app.run(port=5000, debug=True)
