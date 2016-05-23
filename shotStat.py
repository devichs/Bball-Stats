import requests
import json 
import sqlite3

url = "http://stats.nba.com/stats/leaguedashplayerptshot?CloseDefDistRange=&College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&DribbleRange=&GameScope=&GameSegment=&GeneralRange=Overall&Height=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerErperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2015-16&SeasonSegment=&SeasonType=Playoffs&ShotClockRange=&ShotDistRange=&StarterBench=&TeamID=0&TouchTimeRange=&VsConference=&VsDivision=&Weight=&closestDef10="

import sys, os, pickle
file_name = 'result_sets.pickled'

if os.path.isfile(file_name):
  result_sets = pickle.load(open(file_name, 'rb'))
else: 
  r = requests.get(url)
  r.raise_for_status()
  result_sets = r.json()['resultSets'][0]["rowSet"]
  pickle.dump(result_sets, open(file_name, 'wb'))

print(result_sets)
for row in result_sets:
	con = sqlite3.connect("bball.sqlite")
	c = con.cursor()
	c.executescript("""
	drop table if exists bball;
	create table if not exists bball(
	rid integer primary key not null unique,
	reporttime DATETIME default current_timestamp not null,
	PLAYER_ID integer,
	PLAYER_NAME text,
	PLAYER_LAST_TEAM_ID integer,
	PLAYER_LAST_TEAM_ABBREVIATION text,
	AGE integer,
	GP float,
	G float,
	FGA_FREQUENCY float,
	FGM float,
	FGA float,
	FG_PCT float,
	EFG_PCT float,
	FG2A_FREQUENCY float,
	FG2M float,
	FG2A float,
	FG2_PCT float,
	FG3A_FREQUENCY float,
	FG3M float,
	FG3A float,
	FG3_PCT float
	)""")
	con.commit()
	c.execute("""
	insert or ignore into bball (PLAYER_ID,PLAYER_NAME,PLAYER_LAST_TEAM_ID,PLAYER_LAST_TEAM_ABBREVIATION,AGE,GP,G,FGA_FREQUENCY,FGM,FGA,FG_PCT,EFG_PCT,FG2A_FREQUENCY,FG2M,FG2A,FG2_PCT,FG3A_FREQUENCY,FG3M,FG3A,FG3_PCT) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],))
	con.commit()
c.execute("""
select count()ct, player_name from bball group by player_name
""")
results = c.fetchall()
print(results)




