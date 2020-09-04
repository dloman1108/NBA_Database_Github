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

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]

	#Get credentials stored in sql.yaml file (saved in root directory)
	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+'sql.yaml', 'r') as stream:
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

	fp=os.path.dirname(os.path.realpath(__file__))
	team_stats_agg_query=open(fp+'/TeamStatsAgg.sql').read()

	team_stats_agg=pd.read_sql(team_stats_agg_query,engine)

	team_stats_agg.to_sql('team_stats_agg',
						   con=engine,
						   schema='nba',
						   index=False,
						   if_exists='replace',
						   dtype={'team': sa.types.VARCHAR(length=255),
						   		  'team_abbr': sa.types.VARCHAR(length=255),
								  'team_id': sa.types.INTEGER(),
								  'season': sa.types.INTEGER(),
								  'game_type': sa.types.VARCHAR(length=255),
								  'gp': sa.types.INTEGER(),
								  'wins': sa.types.INTEGER(),
								  'losses': sa.types.INTEGER(),
								  'win_pct': sa.types.FLOAT(),
								  'fgm': sa.types.FLOAT(),
								  'fga': sa.types.FLOAT(),
								  'fg_pct': sa.types.FLOAT(),
								  'fg3m': sa.types.FLOAT(),
								  'fg3a': sa.types.FLOAT(),
								  'fg3_pct': sa.types.FLOAT(),
								  'ftm': sa.types.FLOAT(),
								  'fta': sa.types.FLOAT(),
								  'ft_pct': sa.types.FLOAT(),
								  'pts': sa.types.FLOAT(),
								  'reb': sa.types.FLOAT(),
								  'oreb': sa.types.FLOAT(),
								  'dreb': sa.types.FLOAT(),
								  'ast': sa.types.FLOAT(),
								  'stl': sa.types.FLOAT(),
								  'blk': sa.types.FLOAT(),
								  'tov': sa.types.FLOAT(),
								  'pts_off_tov': sa.types.FLOAT(),
								  'fst_brk_pts': sa.types.FLOAT(),
								  'pts_in_pnt': sa.types.FLOAT(),
								  'pf': sa.types.FLOAT(),
								  'tech_fl': sa.types.FLOAT(),
								  'flag_fl': sa.types.FLOAT(),
								  'fgm_opp': sa.types.FLOAT(),
								  'fga_opp': sa.types.FLOAT(),
								  'fg_pct_opp': sa.types.FLOAT(),
								  'fg3m_opp': sa.types.FLOAT(),
								  'fg3a_opp': sa.types.FLOAT(),
								  'fg3_pct_opp': sa.types.FLOAT(),
								  'ftm_opp': sa.types.FLOAT(),
								  'fta_opp': sa.types.FLOAT(),
								  'ft_pct_opp': sa.types.FLOAT(),
								  'pts_opp': sa.types.FLOAT(),
								  'reb_opp': sa.types.FLOAT(),
								  'oreb_opp': sa.types.FLOAT(),
								  'dreb_opp': sa.types.FLOAT(),
								  'ast_opp': sa.types.FLOAT(),
								  'stl_opp': sa.types.FLOAT(),
								  'blk_opp': sa.types.FLOAT(),
								  'tov_opp': sa.types.FLOAT(),
								  'pts_off_tov_opp': sa.types.FLOAT(),
								  'fst_brk_pts_opp': sa.types.FLOAT(),
								  'pts_in_pnt_opp': sa.types.FLOAT(),
								  'pf_opp': sa.types.FLOAT(),
								  'tech_fl_opp': sa.types.FLOAT(),
								  'flag_fl_opp': sa.types.FLOAT(),
								  'mp': sa.types.FLOAT(),
								  'poss': sa.types.FLOAT(),
								  'pace': sa.types.FLOAT(),
								  'off_rtg': sa.types.FLOAT(),
								  'def_rtg': sa.types.FLOAT(),
								  'net_rtg': sa.types.FLOAT(),
								  'fg3_rate': sa.types.FLOAT(),
								  'ft_rate': sa.types.FLOAT(),
								  'efg_pct': sa.types.FLOAT(),
								  'tov_pct': sa.types.FLOAT(),
								  'oreb_pct': sa.types.FLOAT(),
								  'ff_ft_rate': sa.types.FLOAT(),
								  'fg3_rate_opp': sa.types.FLOAT(),
								  'ft_rate_opp': sa.types.FLOAT(),
								  'efg_pct_opp': sa.types.FLOAT(),
								  'tov_pct_opp': sa.types.FLOAT(),
								  'oreb_oct_opp': sa.types.FLOAT(),
								  'ff_ft_rate_opp': sa.types.FLOAT(),
								  'last_update_dts': sa.types.DateTime()})	

def main():
	engine=get_engine()
	calculate_team_stats(engine)
	
	
if __name__ == "__main__":
	main()


