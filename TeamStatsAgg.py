# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 17:21:21 2018

@author: DanLo1108
"""


import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml


def get_engine():
	#Get credentials stored in sql.yaml file (saved in root directory)
	if os.path.isfile('/sql.yaml'):
	    with open("/sql.yaml", 'r') as stream:
		data_loaded = yaml.load(stream)

		#domain=data_loaded['SQL_DEV']['domain']
		user=data_loaded['BBALL_STATS']['user']
		password=data_loaded['BBALL_STATS']['password']
		endpoint=data_loaded['BBALL_STATS']['endpoint']
		port=data_loaded['BBALL_STATS']['port']
		database=data_loaded['BBALL_STATS']['database']

	db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(user,password,endpoint,port,database)
	engine=sa.create_engine(db_string)

	return engine


def calculate_team_stats(engine):
	team_stats_agg_query='''
	with possessions as (
		select
			tb."Team"
			,gs."Season"
			,gs."GameType"
			,.5*((sum(tb."FGA")+0.4*sum(tb."FTA")-1.07*(sum(tb."OREB")*1.0/(sum(tb."OREB")+sum(tb."DREB")))*(sum(tb."FGA")-sum(tb."FGM"))+sum(tb."TOV"))+(sum(tb."FGA_opp")+0.4*sum(tb."FTA_opp")-1.07*(sum(tb."OREB_opp")*1.0/(sum(tb."OREB_opp")+sum(tb."DREB_opp"))) * (sum(tb."FGA_opp")-sum(tb."FGM_opp"))+sum(tb."TOV_opp"))) "Poss"
		from
			nba.team_boxscores tb
		join
			nba.game_summaries gs on tb."GameID"=gs."GameID" and gs."Status"='Final'
		group by
			tb."Team"
			,gs."Season"
			,gs."GameType"
	)

	select
		tb."Team"
		,max(case 
			when tb."Team"=gs."HomeTeamAbbr" then gs."HomeTeamID" 
			when tb."Team"=gs."AwayTeamAbbr" then gs."AwayTeamID" else Null end) "TeamID"
		,gs."Season"
		,gs."GameType"
		,count(*) "GP"
		,sum(case when tb."PTS" > tb."PTS_opp" then 1 else 0 end) "Wins"
		,sum(case when tb."PTS" < tb."PTS_opp" then 1 else 0 end) "Losses"
		,avg(case when tb."PTS" > tb."PTS_opp" then 1.0 else 0 end) "WinPct"
		,avg(tb."FGM"*1.0) "FGM"
		,avg(tb."FGA"*1.0) "FGA"
		,sum(tb."FGM")*1.0/sum(tb."FGA") "FG_Pct"
		,avg(tb."3PTM"*1.0) "FG3M"
		,avg(tb."3PTA"*1.0) "FG3A"
		,sum(tb."3PTM")*1.0/sum(tb."3PTA") "FG3_Pct"
		,avg(tb."FTM"*1.0) "FTM"
		,avg(tb."FTA"*1.0) "FTA"
		,sum(tb."FTM")*1.0/sum(tb."FTA") "FT_Pct"
		,avg(tb."PTS") "PTS"
		,avg(tb."OREB")+avg(tb."DREB") "REB"
		,avg(tb."OREB") "OREB"
		,avg(tb."DREB") "DREB"
		,avg(tb."AST") "AST"
		,avg(tb."STL") "STL"
		,avg(tb."BLK") "BLK"
		,avg(tb."TOV") "TOV"
		,avg(tb."PtsOffTOV") "PtsOffTOV"
		,avg(tb."FstBrkPts") "FstBrkPts"
		,avg(tb."PtsInPnt") "PtsInPnt"
		,avg(tb."PF") "PF"
		,avg(tb."TechF") "TechF"
		,avg(tb."FlagF") "FlagF"
		,avg(tb."FGM_opp"*1.0) "FGM_opp"
		,avg(tb."FGA_opp"*1.0) "FGA_opp"
		,sum(tb."FGM_opp")*1.0/sum(tb."FGA_opp") "FG_Pct_opp"
		,avg(tb."3PTM_opp"*1.0) "FG3M_opp"
		,avg(tb."3PTA_opp"*1.0) "FG3A_opp"
		,sum(tb."3PTM_opp")*1.0/sum(tb."3PTA_opp") "FG3_Pct_opp"
		,avg(tb."FTM_opp"*1.0) "FTM_opp"
		,avg(tb."FTA_opp"*1.0) "FTA_opp"
		,sum(tb."FTM_opp")*1.0/sum(tb."FTA_opp") "FT_Pct_opp"
		,avg(tb."PTS_opp") "PTS_opp"
		,avg(tb."OREB_opp")+avg(tb."DREB_opp") "REB_opp"
		,avg(tb."OREB_opp") "OREB_opp"
		,avg(tb."DREB_opp") "DREB_opp"
		,avg(tb."AST_opp") "AST_opp"
		,avg(tb."STL_opp") "STL_opp"
		,avg(tb."BLK_opp") "BLK_opp"
		,avg(tb."TOV_opp") "TOV_opp"
		,avg(tb."PtsOffTOV_opp") "PtsOffTOV_opp"
		,avg(tb."FstBrkPts_opp") "FstBrkPts_opp"
		,avg(tb."PtsInPnt_opp") "PtsInPnt_opp"
		,avg("PF_opp") "PF_opp"
		,avg("TechF_opp") "TechF_opp"
		,avg("FlagF_opp") "FlagF_opp"
		--Get 4 factors + ratings
		,sum(pb."MP") "MP"
		,p."Poss"
		,48*((p."Poss"*2)/(2*(sum(pb."MP")/5.0))) "Pace"
		,sum(tb."PTS")/p."Poss"*100 "OffRTG"
		,sum(tb."PTS_opp")*100.0/p."Poss" "DefRTG"
		,(sum(tb."PTS")-sum(tb."PTS_opp"))/p."Poss"*100 "NetRTG"
		,sum(tb."3PTA")*1.0/sum(tb."FGA") "FG3_Rate"
		,sum(tb."FTA")*1.0/sum(tb."FGA") "FT_Rate"
		,(sum(tb."FGM")+.5*sum(tb."3PTM"))/sum(tb."FGA") "eFG_Pct"
		,sum(tb."TOV")/(sum(tb."FGA")+.44*sum(tb."FTA")+sum(tb."TOV")) "TOV_Pct"
		,sum(tb."OREB")*1.0/(sum(tb."OREB")+sum(tb."DREB_opp")) "OREB_Pct"
		,sum(tb."FTM")*1.0/sum(tb."FGA") "FF_FT_Rate"
		,sum(tb."3PTA_opp")*1.0/sum(tb."FGA_opp") "FG3_Rate_opp"
		,sum(tb."FTA_opp")*1.0/sum(tb."FGA_opp") "FT_Rate_opp"
		,(sum(tb."FGM_opp")+.5*sum(tb."3PTM_opp"))/sum(tb."FGA_opp") "eFG_Pct_opp"
		,sum(tb."TOV_opp")/(sum(tb."FGA_opp")+.44*sum(tb."FTA_opp")+sum(tb."TOV_opp")) "TOV_Pct_opp"
		,sum(tb."OREB_opp")*1.0/(sum(tb."OREB_opp")+sum(tb."DREB")) "OREB_Pct_opp"
		,sum(tb."FTM_opp")*1.0/sum(tb."FGA_opp") "FF_FT_Rate_opp"
	from
		nba.team_boxscores tb
	join
		nba.game_summaries gs on tb."GameID"=gs."GameID" and gs."Status"='Final'
	left join
		(select "GameID","Team",sum(cast("MP" as float)) "MP" from nba.player_boxscores group by "GameID","Team") pb on tb."GameID"=pb."GameID" and tb."Team"=pb."Team"
	join
		possessions p on tb."Team"=p."Team" and gs."Season"=p."Season" and gs."GameType"=p."GameType"
	group by
		tb."Team"
		,gs."Season"
		,gs."GameType"
		,p."Poss"
	'''

	team_stats_agg=pd.read_sql(team_stats_agg_query,engine)

	team_stats_agg.to_sql('team_stats_agg',con=engine,schema='nba',index=False,if_exists='replace')
	

def main():
    engine=get_engine()
    calculate_team_stats(engine)
    
    
if __name__ == "__main__":
    main()


