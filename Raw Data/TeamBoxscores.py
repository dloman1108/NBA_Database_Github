# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 12:13:57 2016

@author: DanLo1108
"""


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import os
import yaml
import string as st

import sqlalchemy as sa
from urllib.request import urlopen

def get_made(x,var):
	x_var=x[var]
	try:
		return int(x_var[:x_var.index('-')])
	except:
		return np.nan
			
def get_attempts(x,var):
	x_var=x[var]
	try:
		return int(x_var[x_var.index('-')+1:])
	except:
		return np.nan
		
		
def get_possessions(x):
	return .5*((x.fga+0.4*x.fta-1.07*(x.oreb*1.0/(x.oreb+x.dreb_opp))*(x.fga-x.fgm)+x.tov)+
			   (x.fga_opp+0.4*x.fta_opp-1.07*(x.oreb_opp*1.0/(x.dreb))*(x.fga_opp-x.fgm_opp)+x.tov_opp))
		
		
def append_team_boxscores(game_id,engine):
	#if 1==1:
	
	#gameId='400899617'
	
	url='http://www.espn.com/nba/matchup?gameId='+str(game_id)
	
	page = urlopen(url)
	
	
	content=page.read()
	soup=BeautifulSoup(content,'lxml')

	tables=soup.find_all('table')
	
	try:
		results_head=[re.sub('\t|\n','',el.string) for el in tables[0].find_all('td')]
		results_body=[re.sub('\t|\n','',el.string) for el in tables[1].find_all('td')]
	except:
		results_head=[el.string for el in tables[0].find_all('td')]
		results_body=[el.string for el in tables[1].find_all('td')]
		
	results_head_split=np.array_split(results_head,len(results_head)/5.)
	
	#results_body[0::3]
	#results_body[1::3]
	#results_body[2::3]
	
	tm1=results_head_split[0][0]
	tm2=results_head_split[1][0]
	
	tm1_pts=results_head_split[0][-1]
	tm2_pts=results_head_split[1][-1]
	
	if len(results_body) == 60:
		team_stats_df=pd.DataFrame([([tm1,tm1_pts]+results_body[:-3][1::3]),
					  ([tm2,tm2_pts]+results_body[:-3][2::3])],
					columns=['team_abbr','pts','fg','fg_pct','fg3','fg3_pct',
							 'ft','ft_pct','reb','oreb','dreb','ast','stl',
							 'blk','tov','pts_off_tov','fst_brk_pts','pts_in_pnt',
							 'pf','tech_fl','flag_fl'])
	else:
		team_stats_df=pd.DataFrame([([tm1,tm1_pts]+results_body[1::3]),
					  ([tm2,tm2_pts]+results_body[2::3])],
					columns=['team_abbr','pts','fg','fg_pct','fg3','fg3_pct',
							 'ft','ft_pct','reb','oreb','dreb','ast','stl',
							 'blk','tov','pts_off_tov','fst_brk_pts','pts_in_pnt',
							 'pf','tech_fl','flag_fl'])
						 
	for col in team_stats_df:
		try:
			team_stats_df[col]=list(map(lambda x: int(x), team_stats_df[col]))
		except:
			continue
	
	team_stats_df['game_id']=game_id
	
	team_stats_df['fgm']=team_stats_df.apply(lambda x: get_made(x,'fg'), axis=1)
	team_stats_df['fga']=team_stats_df.apply(lambda x: get_attempts(x,'fg'), axis=1)
	
	team_stats_df['fg3m']=team_stats_df.apply(lambda x: get_made(x,'fg3'), axis=1)
	team_stats_df['fg3a']=team_stats_df.apply(lambda x: get_attempts(x,'fg3'), axis=1)
	
	team_stats_df['ftm']=team_stats_df.apply(lambda x: get_made(x,'ft'), axis=1)
	team_stats_df['fta']=team_stats_df.apply(lambda x: get_attempts(x,'ft'), axis=1)
	
	team_stats_df['opp_team_abbr']=team_stats_df.team_abbr.tolist()[::-1]
	
	team_stats_df=team_stats_df.merge(team_stats_df.drop(['team_abbr','game_id'],axis=1),left_on='team_abbr',right_on='opp_team_abbr',suffixes=('', '_opp')).drop('opp_team_abbr_opp',axis=1)   
	
	team_stats_df['poss']=team_stats_df.apply(lambda x: get_possessions(x),axis=1)
	team_stats_df=team_stats_df.merge(team_stats_df[['opp_team_abbr','poss']],left_on='team_abbr',right_on='opp_team_abbr',suffixes=('', '_opp')).drop('opp_team_abbr_opp',axis=1)   
	
	team_stats_df['ortg']=team_stats_df.pts*100/team_stats_df.poss
	team_stats_df['drtg']=team_stats_df.pts_opp*100/team_stats_df.poss
	team_stats_df['net_rtg']=(team_stats_df.pts-team_stats_df.pts_opp)*100/team_stats_df.poss
	
	column_order=['game_id', 'team_abbr', 'pts', 'fg', 'fgm', 'fga', 'fg_pct', 'fg3', 'fg3_pct', 
				  'fg3m','fg3a', 'ft', 'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb',
				  'ast', 'stl', 'blk', 'tov', 'pts_off_tov', 'fst_brk_pts','pts_in_pnt', 'pf', 
				  'tech_fl', 'flag_fl', 'opp_team_abbr', 'pts_opp', 'fg_opp', 'fgm_opp', 'fga_opp',
				  'fg_pct_opp', 'fg3_opp', 'fg3m_opp', 'fg3a_opp', 'fg3_pct_opp', 'ft_opp', 
				  'ftm_opp', 'fta_opp', 'ft_pct_opp', 'oreb_opp', 'dreb_opp', 'reb_opp',
				  'ast_opp', 'stl_opp', 'blk_opp', 'tov_opp', 'pts_off_tov_opp',
				  'fst_brk_pts_opp', 'pts_in_pnt_opp', 'pf_opp', 'tech_fl_opp', 'flag_fl_opp',
				  'poss', 'poss_opp', 'ortg', 'drtg', 'net_rtg']
	
	team_stats_df[column_order].to_sql('team_boxscores',
						 con=engine,
						 schema='nba',
						 index=False,
						 if_exists='append',
						 dtype={'game_id': sa.types.INTEGER(),
								'team_abbr': sa.types.VARCHAR(length=255),
								'pts': sa.types.INTEGER(),
								'fg': sa.types.VARCHAR(length=255),
								'fgm': sa.types.INTEGER(),
								'fga': sa.types.INTEGER(),
								'fg_pct': sa.types.FLOAT(),
								'fg3': sa.types.VARCHAR(length=255),
								'fg3m': sa.types.INTEGER(),
								'fg3a': sa.types.INTEGER(),
								'fg3_pct': sa.types.FLOAT(),
								'ft': sa.types.VARCHAR(length=255),
								'ftm': sa.types.INTEGER(),
								'fta': sa.types.INTEGER(),
								'ft_pct': sa.types.FLOAT(),
								'oreb': sa.types.INTEGER(),
								'dreb': sa.types.INTEGER(),
								'reb': sa.types.INTEGER(),
								'ast': sa.types.INTEGER(),
								'stl': sa.types.INTEGER(),
								'blk': sa.types.INTEGER(),
								'tov': sa.types.INTEGER(),
								'pts_off_tov': sa.types.INTEGER(),
								'fst_brk_pts': sa.types.INTEGER(),
								'pts_in_pnt': sa.types.INTEGER(),
								'pf': sa.types.INTEGER(),
								'tech_fl': sa.types.INTEGER(),
								'flag_fl': sa.types.INTEGER(),
								
								'opp_team_abbr': sa.types.VARCHAR(length=255),
								'pts_opp': sa.types.INTEGER(),
								'fg_opp': sa.types.VARCHAR(length=255),
								'fgm_opp': sa.types.INTEGER(),
								'fga_opp': sa.types.INTEGER(),
								'fg_pct_opp': sa.types.FLOAT(),
								'fg3_opp': sa.types.VARCHAR(length=255),
								'fg3m_opp': sa.types.INTEGER(),
								'fg3a_opp': sa.types.INTEGER(),
								'fg3_pct_opp': sa.types.FLOAT(),
								'ft_opp': sa.types.VARCHAR(length=255),
								'ftm_opp': sa.types.INTEGER(),
								'fta_opp': sa.types.INTEGER(),
								'ft_pct_opp': sa.types.FLOAT(),
								'oreb_opp': sa.types.INTEGER(),
								'dreb_opp': sa.types.INTEGER(),
								'reb_opp': sa.types.INTEGER(),
								'ast_opp': sa.types.INTEGER(),
								'stl_opp': sa.types.INTEGER(),
								'blk_opp': sa.types.INTEGER(),
								'tov_opp': sa.types.INTEGER(),
								'pts_off_tov_opp': sa.types.INTEGER(),
								'fst_brk_pts_opp': sa.types.INTEGER(),
								'pts_in_pnt_opp': sa.types.INTEGER(),
								'pf_opp': sa.types.INTEGER(),
								'tech_fl_opp': sa.types.INTEGER(),
								'flag_fl_opp': sa.types.INTEGER(),
								
								'poss': sa.types.FLOAT(),
								'poss_opp': sa.types.FLOAT(),
								'ortg': sa.types.FLOAT(),
								'drtg': sa.types.FLOAT(),
								'net_rtg': sa.types.FLOAT()})
		
	
def get_engine():

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]

	if os.path.isfile(yaml_fp+'/sql.yaml'):
		with open(yaml_fp+'/sql.yaml', 'r') as stream:
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


	
def get_gameids(engine):
	
	game_id_query='''
	select distinct
		gs.season
		,gs.game_id
	from
		nba.game_summaries gs
	left join
		nba.team_boxscores p on gs.game_id=p.game_id
	left join
		nba.bad_gameids b on gs.game_id=b.game_id and b.table='team_boxscores'
	where
		p.game_id is Null
		and b.game_id is Null
		and gs.status='Final'
	order by
		gs.season
	'''
	
	game_ids=pd.read_sql(game_id_query,engine)
	
	return game_ids.game_id.tolist()


def update_team_boxscores(engine,game_id_list):
	cnt=0
	print('Total Games: ',len(game_id_list))
	for game_id in game_id_list:
	
		try:
			append_team_boxscores(game_id,engine)
			cnt+=1
			if np.mod(cnt,100)==0:
				print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
			
		except:
			bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['team_boxscores']})
			bad_gameid_df.to_sql('bad_gameids',
								  con=engine,
								  schema='nba',
								  index=False,
								  if_exists='append',
								  dtype={'game_id': sa.types.INTEGER(),
										 'table': sa.types.VARCHAR(length=255)})
			cnt+=1
			if np.mod(cnt,100) == 0:
				print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
			continue
		
		
def main():
	engine=get_engine()
	game_ids=get_gameids(engine)
	update_team_boxscores(engine,game_ids)
	
	
	
if __name__ == "__main__":
	main()
	
