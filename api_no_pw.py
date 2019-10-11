import flask
import psycopg2
from flask import Flask, request, jsonify, render_template
import simplejson as json
from pandas import DataFrame
import pandas as pd


app = flask.Flask(__name__)
app.config["DEBUG"] = True

def db_connect(sql):
	connection = psycopg2.connect(user = "", password = "", host = "165.22.176.224", port = "5432", database = "nhl_stats")
	connection.autocommit = True
	try:
		result = pd.read_sql_query(sql, connection)
		return result
	except (Exception, psycopg2.Error) as error :
		print ("Error while connecting to PostgreSQL", error)
	finally:
		if(connection):
			connection.close()
			print("PostgreSQL connection is closed")


@app.route('/player', methods=['GET'])
def player():
	sql1 = """SELECT pi.player_id, pi.firstName, pi.lastName, ti.shortname as "Team City", ti.teamname as "Team Name", format('%s - %s', left(ga.season, 4), right(ga.season, -4)) as Season, pi.nationality, pi.primaryposition, pi.birthcity, pi.birthdate,  round(avg("timeOnIce"),2) as AverageTimeOnIce, sum(assists) as Assists, sum(goals) as Goals, sum(shots) as Shots, sum(hits) as Hits, sum("powerPlayGoals") as PowerPlayGoals, sum("powerPlayAssists") as PowerPlayAssists, sum("penaltyMinutes") as PenaltyMinuutes, sum("faceOffWins") as FaceOffWins, sum("faceoffTaken") as FaceOffTaken, sum(takeaways) as Takeaways, sum(giveaways)as Giveaways, sum("shortHandedGoals") as ShortHandedGoals, sum("shortHandedAssists") as ShortHandedAssists, sum("blockedShots") as BlockedShots, sum("plusMinus")as PlusMinus, sum("evenTimeOnIce") as EvenTimeOnIce, sum("shortHandedTimeOnIce") as ShortHandedTimeOnIce, sum("powerPlayTimeOnIce") as PowerPlayTimeOnIce FROM public.player_info pi JOIN public.game_skater_stats gst ON pi.player_id = gst.player_id JOIN public.team_info ti ON ti.team_id = gst.team_id JOIN public.game ga ON gst.game_id = ga.game_id """

	sql2 = " GROUP BY pi.player_id, pi.firstName, pi.lastName, pi.nationality, pi.primaryposition, pi.birthcity, pi.birthdate, ti.shortname, ti.teamname, ga.season;"

	where = []
	x = 'doug'
	
	if 'firstname' in request.args:
		where.append("pi.firstName = '" + request.args.get('firstname') + "'")
	if 'lastname' in request.args:
		where.append("pi.lastName = '" + request.args.get('lastname') + "'")
	if 'nationality' in request.args:
		where.append("pi.nationality = '" + request.args.get('nationality') + "'")
	if 'primaryposition' in request.args:
		where.append("pi.primaryposition = '" + request.args.get('primaryposition') + "'")
	if 'teamname' in request.args:
		where.append("ti.teamname = '" + request.args.get('teamname') + "'")
	
	if len(where) > 0:
		where = ' AND '.join(where)
		where = "WHERE " + where
	else:
		where = ""

	sql = sql1 + where + sql2
	result = db_connect(sql)
	return result.to_json(orient='records')


# @app.route('/game_shifts', methods=['GET'])
# def game_shifts():


app.run(debug=True)