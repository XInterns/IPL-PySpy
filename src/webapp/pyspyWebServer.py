from flask import Flask, jsonify, request
from src.webapp.webapps import *

app = Flask(__name__)
matchDF = get_match_df()
playerPerformanceObj = PlayerPerformance()


########### DropDown Lists ###########
seasonList = get_dropdown_list(matchDF,"season",1,"int")
lboundList = get_dropdown_list(matchDF,"season",1,"int")
uboundList = get_dropdown_list(matchDF,"season",1,"int")
year_list = get_dropdown_list(matchDF,"season",1,"string")
player_names = playerPerformanceObj.getPlayerNames()
teams = ['Rajasthan Royals','Chennai Super Kings','Deccan Chargers','Gujarat Lions','Delhi Daredevils','Mumbai Indians','Kochi Tuskers Kerala','Royal Challengers Bangalore','Pune Warriors','Rising Pune Supergiants','Sunrisers Hyderabad','Kolkata Knight Riders','Kings XI Punjab']


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


@app.route("/DreamTeam/help", methods=["GET"])
def returnDreamTeamHelp():
    return render_template("dreamteamHelp.html")


@app.route("/overallStandings", methods=["GET"])
def returnOverallStandings():
	args = request.args
	season = args['season']
	return jsonify({"Overall_Standings_"+season: overall_rank_jsonify(matchDF, season)})


@app.route("/performanceConsistency", methods=["GET"])
def returnPerformanceConsistencies():
	args = request.args
	lbound = int(args['lbound'])
	ubound = int(args['ubound'])
	return jsonify({"Performance_Consistency_"+str(lbound)+"_to_"+str(ubound): consistency_jsonify(matchDF, lbound, ubound)})


@app.route("/TeamVsTeamWinPercentage", methods=["GET"])
def returnTeamVsTeamWinPercentage():
	args = request.args
	team1 = args['team1']
	team2 = args['team2']
	return jsonify({"Team_Vs_Team_Win_Percentage_"+team1+"_VS_"+team2: team_vs_team_jsonify(matchDF, team1, team2)})


@app.route("/PlayerPerformance", methods=["GET"])
def returnPlayerPerformance():
	args = request.args
	player = args['player']
	return jsonify({"Player_Performance_"+player: Player_Performance_jsonify(player)})


@app.route("/DreamTeam", methods=["GET"])
def returnDreamTeam():
    args = request.args
    season1 = args['season1']
    season2 = args['season2']
    return jsonify({"Dream_Team_"+str(season1)+"_"+str(season2): dream_team_jsonify(season1, season2)})


########### WebApp Routing & Functionality ###########
@app.route("/PlayerPerformance/webapp")
def returnPlayerPerformApp():
    # Determine the selected feature
    player1 = request.args.get("player1")
    if player1 == None:
        player1 = "V Kohli"

    player2 = request.args.get("player2")
    if player2 == None:
        player2 = "Average"

    # Create the plot
    plot = playerPerformanceObj.create_figure_player_performance(player1, player2)
        
    # Embed plot into HTML via Flask Render
    return render_template("playerperformance.html", plot=plot, player_names=player_names, player1=player1, player2=player2)


@app.route("/seasonOverview/webapp")
def returnSeasonOverviewWebApp():
    # Determine the selected feature
    season = request.args.get("season")
    if season == None:
        season = 2013
    else:
        season = int(season)

    # Create the plot
    plot = create_figure_season_overview(matchDF, season)
    
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
    plot = create_figure_overall_ranks(matchDF, season)
        
    # Embed plot into HTML via Flask Render
    script, div = components(plot)
    return render_template("overallRanks.html", script=script, div=div, seasonList=seasonList, season=season)


@app.route("/performanceConsistency/webapp")
def returnPerformanceConsistencyWebApp():
    # Determine the selected feature
    lbound = request.args.get("lbound")
    ubound = request.args.get("ubound")
    
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
    plot = create_figure_performance_consistency(matchDF, lbound, ubound)

    # Embed plot into HTML via Flask Render
    # script, div = components(plot)
    # return render_template("performanceConsistency.html",\
    #         script=script, div=div, lboundList=lboundList,\
    #         uboundList=uboundList, lbound=lbound,\
    #         ubound=ubound)
    return render_template("performanceConsistency.html", plot=plot,\
            lboundList=lboundList,uboundList=uboundList,\
            lbound=lbound,ubound=ubound)


@app.route("/TeamVsTeamWinPercentage/webapp")
def returnTeamVsTeamWinPercentageApp():
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
        result = create_figure_team_vs_team_win_percentage(matchDF,team1,team2)
        if result == None:
            flag2=1
        
    return render_template("teamVsTeamWinPercentage.html",result=result,team1=team1,team2=team2,teams=teams,flag=flag,flag2=flag2)


@app.route('/DreamTeam/webapp')
def returnDreamTeamWebApp():

    lbound= request.args.get("lbound")
    ubound= request.args.get("ubound")
    if lbound == None:
       lbound = "2008"
    
    if ubound == None:
        ubound = "2016"

    lseason = int(lbound)
    useason = int(ubound)
    if(lseason > useason):
        lseason^=useason
        useason^=lseason
        lseason^=useason
    lbound = str(lseason)
    ubound = str(useason)
    
    plot = create_team(lbound,ubound)
        

    script, div = components(plot)
    return render_template("dreamteam.html", script=script, div=div, year_list=year_list,lbound=lbound,ubound=ubound)


if __name__ == "__main__":
	app.run(port=5000)
