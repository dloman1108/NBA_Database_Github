# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 11:03:47 2018

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
	
	

def calculate_player_stats(engine):
	player_stats_agg_query='''

	select 
	    pb."Player"
	    ,pb."PlayerID"
	    ,pb."Team"
	    ,gs."Season"
	    ,gs."GameType"
	    ,count(*) "GP"
	    ,sum(pb."MP") "MP"
	    ,avg(pb."PTS") "PPG"
	    ,avg(pb."FGM") "FGM"
	    ,avg(pb."FGA") "FGA"
	    ,sum(pb."FGM")*1.0/sum(pb."FGA") "FG_Pct"
	    ,avg(pb."3PTM") "FG3M"
	    ,avg(pb."3PTA") "FG3A"
	    ,case when sum(pb."3PTA") > 0 then sum(pb."3PTM")*1.0/sum(pb."3PTA") else Null end "FG3_Pct"
	    ,avg(pb."FTM") "FTM"
	    ,avg(pb."FTA") "FTA"
	    ,sum(pb."FTM")*1.0/sum(pb."FTA") "FT_Pct"
	    ,avg(pb."OREB") "OREB"
	    ,avg(pb."DREB") "DREB"
	    ,avg(pb."REB") "REB"
	    ,avg(pb."AST") "AST"
	    ,avg(pb."STL") "STL"
	    ,avg(pb."BLK") "BLK"
	    ,avg(pb."TOV") "TOV"
	    ,avg(pb."PF") "PF"
	    ,sum(pb."PlusMinus") "RawPlusMinus"
	    ,sum(pb."PTS")*36.0/sum(pb."MP") "PTS_36"
	    ,sum(pb."FGM")*36.0/sum(pb."MP") "FGM_36"
	    ,sum(pb."FGA")*36.0/sum(pb."MP") "FGA_36"
	    ,sum(pb."3PTM")*36.0/sum(pb."MP") "FG3M_36"
	    ,sum(pb."3PTA")*36.0/sum(pb."MP") "FG3A_36"
	    ,sum(pb."FTM")*36.0/sum(pb."MP") "FTM_36"
	    ,sum(pb."FTA")*36.0/sum(pb."MP") "FTA_36"
	    ,sum(pb."OREB")*36.0/sum(pb."MP") "OREB_36"
	    ,sum(pb."DREB")*36.0/sum(pb."MP") "DREB_36"
	    ,sum(pb."REB")*36.0/sum(pb."MP") "REB_36"
	    ,sum(pb."AST")*36.0/sum(pb."MP") "AST_36"
	    ,sum(pb."STL")*36.0/sum(pb."MP") "STL_36"
	    ,sum(pb."BLK")*36.0/sum(pb."MP") "BLK_36"
	    ,sum(pb."TOV")*36.0/sum(pb."MP") "TOV_36"
	    ,sum(pb."PF")*36.0/sum(pb."MP") "PF_36"
	    ,(sum(pb."FGM")+sum(pb."3PTM")*.5)/sum(pb."FGA") "eFG_Pct"
	    ,sum(pb."PTS")/(2*(sum(pb."FGA")+0.44*sum(pb."FTA"))) "TS_Pct"
	    ,sum(pb."3PTA")*1.0/sum(pb."FGA") "FG3_Rate"
	    ,sum(pb."FTA")*1.0/sum(pb."FGA") "FT_Rate"
	    ,(sum(pb."OREB")*(tsa."MP"/5.0))/(sum(pb."MP")*(tsa."OREB"*tsa."GP"+tsa."DREB_opp"*tsa."GP")) "OREB_Pct"
	    ,(sum(pb."DREB")*(tsa."MP"/5.0))/(sum(pb."MP")*(tsa."DREB"*tsa."GP"+tsa."OREB_opp"*tsa."GP")) "DREB_Pct"
	    ,(sum(pb."REB")*(tsa."MP"/5.0))/(sum(pb."MP")*(tsa."REB"*tsa."GP"+tsa."REB_opp"*tsa."GP")) "REB_Pct"
	    ,sum(pb."AST")*1.0/(((sum(pb."MP")/(tsa."MP"/5))*tsa."FGM"*tsa."GP")-sum(pb."FGM")) "AST_Pct"
	    ,sum(pb."STL")*1.0/((sum(pb."MP")/(tsa."MP"/5))*tsa."Poss") "STL_Pct"
	    ,sum(pb."BLK")*1.0/((sum(pb."MP")/(tsa."MP"/5))*(tsa."FGA_opp"*tsa."GP"-tsa."FG3A_opp"*tsa."GP")) "BLK_Pct"
	    ,sum(pb."TOV")*1.0/(sum(pb."FGA")+0.44*sum(pb."FTA")+sum(pb."TOV")) "TOV_Pct"
	    ,(sum(pb."FGA")+0.44*sum(pb."FTA")+sum(pb."TOV"))/((sum(pb."MP")/(tsa."MP"/5))*(tsa."FGA"*tsa."GP"+0.44*tsa."FTA"*tsa."GP"+tsa."TOV"*tsa."GP")) "USG_Pct"    
	from 
	    nba.player_boxscores pb 
	join
	    nba.game_summaries gs on pb."GameID"=gs."GameID"
	left join
		nba.team_stats_agg tsa on pb."Team"=tsa."Team" and gs."GameType"=tsa."GameType" and gs."Season"=tsa."Season"
	where 1=1
	    and pb."DNP_Reason" is null
	group by
	    pb."Player"
	    ,pb."PlayerID"
	    ,pb."Team"
	    ,gs."Season"
		,gs."GameType"
		,tsa."MP"
		,tsa."GP"
		,tsa."DREB"
		,tsa."OREB_opp"
		,tsa."OREB"
		,tsa."DREB_opp"
		,tsa."REB"
		,tsa."REB_opp"
		,tsa."FGM"
		,tsa."Poss"
		,tsa."FGA_opp"
		,tsa."FG3A_opp"
		,tsa."FGA"
		,tsa."FTA"
		,tsa."TOV"
	having
	    sum(pb."FGA") > 0 and sum(pb."FTA") > 0 and sum(pb."TOV") > 0

	'''

	player_stats_agg=pd.read_sql(player_stats_agg_query,engine)


	player_stats_agg.to_sql('player_stats_agg',con=engine,schema='nba',index=False,if_exists='replace')


def main():
    engine=get_engine()
    calculate_player_stats(engine)
    
    
if __name__ == "__main__":
    main()


