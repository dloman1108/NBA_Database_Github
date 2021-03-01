# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 21:43:50 2016

@author: DanLo1108
"""


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import sqlalchemy as sa
import os
import yaml

from urllib.request import urlopen

#Break FG and FT down into integers
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
				
				
def append_boxscores(game_id,engine):

	url='http://www.espn.com/nba/boxscore?gameId='+str(game_id)
	
	
	page = urlopen(url)
	
	
	content=page.read()
	soup=BeautifulSoup(content,'lxml')
	
	
	tables=soup.find_all('table')
	
	results_head=[re.sub('\t|\n','',el.string) for el in tables[0].find_all('td')]        
	results_head_split=np.array_split(results_head,len(results_head)/5.)

	for ind in [2,1]:
		results=[el.string for el in tables[ind].find_all('td')]

		try:
			ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and ('DNP-' in results[i] or 'Has not entered game' in results[i] or 'Did not play' in results[i])])-1
		except:
			ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])

		ind_team=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])

		player_stats_df=pd.DataFrame(np.array_split(results[:ind_stop],ind_stop/15.),
						columns=['player','mp','fg','fg3','ft',
								 'oreb','dreb','reb','ast','stl','blk',
								 'tov','pf','plus_minus','pts'])

		for col in player_stats_df:
			try:
				player_stats_df[col]=list(map(lambda x: float(x),player_stats_df[col]))
			except:
				continue
		try:
			if ind_stop != ind_team:
				dnp_df=pd.DataFrame(np.array_split(results[ind_stop:ind_team],(ind_team-ind_stop)/2.),
					   columns=['player','dnp_reason'])
			else:
				dnp_df=pd.DataFrame(columns=['player','dnp_reason'])
		except:
			dnp_df=pd.DataFrame(columns=['player','dnp_reason'])
				
		player_stats_df=player_stats_df.append(dnp_df).reset_index(drop=True)
		
		player_stats_df['player']=[el.string for el in tables[ind].find_all('span')][0::3][:len(player_stats_df)]
	
		try:
			player_stats_df['player_id']=[el['href'][el['href'].find('id')+3:el['href'].find('id')+3+el['href'][el['href'].find('id')+3:].find('/')] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]
		except:
			player_stats_df['player_id']=[el['href'][36:] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]          
		#player_stats_df['PlayerAbbr']=[el['href'][36:][el['href'][36:].index('/')+1:] for el in tables[ind].find_all('a',href=True)][:len(player_stats_df)]      
		
		try:
			player_stats_df['position']=[el.string for el in tables[ind].find_all('span')][2::3][:len(player_stats_df)]
			
		except:
			spans=[el.string for el in tables[ind].find_all('span')]
			pos=[]
			for i in range(1,len(spans)):
				if spans[i] in ['PG','SG','SF','PF','C','G','F']:
					pos.append(spans[i])
				elif spans[i-1] not in ['PG','SG','SF','PF','C','G','F'] and spans[i] not in ['PG','SG','SF','PF','C','G','F'] and spans[i] != spans[i-1]:
					pos.append(None)
				
			if len(pos)==len(player_stats_df):
				player_stats_df['position']=pos
			else:
				player_stats_df['position']=pos+[None]
			
		player_stats_df=player_stats_df.replace('-----','0-0').replace('--',0)
		
		player_stats_df['team_abbr']=results_head_split[ind-1][0]
		player_stats_df['game_id']=game_id
				
				
		player_stats_df['fgm']=player_stats_df.apply(lambda x: get_made(x,'fg'), axis=1)
		player_stats_df['fga']=player_stats_df.apply(lambda x: get_attempts(x,'fg'), axis=1)
		
		player_stats_df['fg3m']=player_stats_df.apply(lambda x: get_made(x,'fg3'), axis=1)
		player_stats_df['fg3a']=player_stats_df.apply(lambda x: get_attempts(x,'fg3'), axis=1)
		
		player_stats_df['ftm']=player_stats_df.apply(lambda x: get_made(x,'ft'), axis=1)
		player_stats_df['fta']=player_stats_df.apply(lambda x: get_attempts(x,'ft'), axis=1)
		
		player_stats_df['starter_flg']=[1.0]*5+[0.0]*(len(player_stats_df)-5)
		
		column_order=['game_id','player','player_id','position','team_abbr','starter_flg','mp',
					  'fg','fgm','fga','fg3','fg3m','fg3a','ft','ftm','fta','oreb','dreb',
					  'reb','ast','stl','blk','tov','pf','plus_minus','pts','dnp_reason']
		
		player_stats_df[column_order].to_sql('player_boxscores_nightly',
											 con=engine,
											 schema='nba_sandbox',
											 index=False,
											 if_exists='replace',
											 dtype={'game_id': sa.types.INTEGER(),
													'player': sa.types.VARCHAR(length=255),
													'player_id': sa.types.INTEGER(),
													'position': sa.types.CHAR(length=2),
													'team_abbr': sa.types.VARCHAR(length=255),
													'starter_flg': sa.types.BOOLEAN(),
													'mp': sa.types.INTEGER(),
													'fg': sa.types.VARCHAR(length=255),
													'fgm': sa.types.INTEGER(),
													'fga': sa.types.INTEGER(),
													'fg3': sa.types.VARCHAR(length=255),
													'fg3m': sa.types.INTEGER(),
													'fg3a': sa.types.INTEGER(),
													'ft': sa.types.VARCHAR(length=255),
													'ftm': sa.types.INTEGER(),
													'fta': sa.types.INTEGER(),
													'oreb': sa.types.INTEGER(),
													'dreb': sa.types.INTEGER(),
													'reb': sa.types.INTEGER(),
													'ast': sa.types.INTEGER(),
													'stl': sa.types.INTEGER(),
													'blk': sa.types.INTEGER(),
													'tov': sa.types.INTEGER(),
													'pf': sa.types.INTEGER(),
													'plus_minus': sa.types.INTEGER(),
													'pts': sa.types.INTEGER(),
													'dnp_reason': sa.types.VARCHAR(length=255)})    


def get_engine():

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]
	
	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+'sql.yaml', 'r') as stream:
			data_loaded = yaml.load(stream)
			
			
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
		game_id
	from
		nba_sandbox.game_summaries_nightly
	'''
	
	game_ids=pd.read_sql(game_id_query,engine)
	
	return game_ids.game_id.tolist()


def update_player_boxscores(engine,game_id_list):
	cnt=0
	print('Total Games: ',len(game_id_list))
	for game_id in game_id_list:
		append_boxscores(game_id,engine)
		
		
def main():
	engine=get_engine()
	game_ids=get_gameids(engine)
	update_player_boxscores(engine,game_ids)
	
	
	
if __name__ == "__main__":
	main()


