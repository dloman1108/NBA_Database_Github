#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue May 15 18:19:55 2018

@author: dh08loma
"""


import sqlalchemy as sa
import pandas as pd
import numpy as np
import yaml
import os
import functools
import re
import pickle
import sklearn

#Get credentials stored in sql.yaml file (saved in root directory)
def get_engine(fp):

	#Yaml stored in directory above script directory (where repository was cloned)
	yaml_fp=fp[:fp.index('NBA-Database')]

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


#Gets sql queries to use in functions 
def get_sql(fp):

	#Read from sql files
	starters_qry=open(fp+'/starters.sql').read()
	subs_qry=open(fp+'/subs.sql').read()
	win_prob_qry=open(fp+'/win_probability.sql').read()
	lineups_qry=open(fp+'/lineups.sql').read()

	#Replace % with %% for python readability
	subs_qry=re.sub('%','%%',subs_qry)
	lineups_qry=re.sub('%','%%',lineups_qry)
	win_prob_qry=re.sub('%','%%',win_prob_qry)

	return starters_qry,subs_qry,lineups_qry,win_prob_qry


#Gets dataframe of starters
def get_starters(game_id,engine,starters_qry):

	starters_df=pd.read_sql(starters_qry.format(game_id),engine)
	return starters_df


def get_subs(game_id,engine,subs_qry):

	subs_df=pd.read_sql(subs_qry.format(game_id),engine)
	return subs_df.sort_values('game_time').reset_index(drop=True)


#Writes to lineups_stage table
def write_game_lineup(game_lineups_df,game_subs_df,engine):

	game_subs_df=game_subs_df.sort_values(['game_time','subbed_in_player_id'])
	for ind in game_subs_df.index.values:
		sub_df=game_lineups_df.loc[[game_lineups_df.index.values[-1]]].copy()
		sub_df['play']=game_subs_df.loc[ind].play
		sub_df['time']=game_subs_df.loc[ind].time
		sub_df['quarter']=game_subs_df.loc[ind].quarter
		
		if game_subs_df.loc[ind].team_abbr==game_lineups_df.home_team_abbr.tolist()[0]:
			
			if game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.home_player1_id.tolist()[-1]:
				sub_df['home_player1']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['home_player1_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.home_player2_id.tolist()[-1]:
				sub_df['home_player2']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['home_player2_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.home_player3_id.tolist()[-1]:
				sub_df['home_player3']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['home_player3_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.home_player4_id.tolist()[-1]:
				sub_df['home_player4']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['home_player4_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.home_player5_id.tolist()[-1]:
				sub_df['home_player5']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['home_player5_id']=game_subs_df.loc[ind].subbed_in_player_id
			#Check for if there is a NULL player being replaced
			else:
				if game_lineups_df.home_player1_id.tolist()[-1] == 0:
					sub_df['home_player1']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['home_player1_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.home_player2_id.tolist()[-1] == 0:
					sub_df['home_player2']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['home_player2_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.home_player3_id.tolist()[-1] == 0:
					sub_df['home_player3']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['home_player3_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.home_player4_id.tolist()[-1] == 0:
					sub_df['home_player4']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['home_player4_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.home_player5_id.tolist()[-1] == 0:
					sub_df['home_player5']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['home_player5_id']=game_subs_df.loc[ind].subbed_in_player_id
				
		if game_subs_df.loc[ind].team_abbr==game_lineups_df.away_team_abbr.tolist()[0]:
			
			if game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.away_player1_id.tolist()[-1]:
				sub_df['away_player1']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['away_player1_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.away_player2_id.tolist()[-1]:
				sub_df['away_player2']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['away_player2_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.away_player3_id.tolist()[-1]:
				sub_df['away_player3']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['away_player3_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.away_player4_id.tolist()[-1]:
				sub_df['away_player4']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['away_player4_id']=game_subs_df.loc[ind].subbed_in_player_id
				
			elif game_subs_df.loc[ind].subbed_out_player_id==game_lineups_df.away_player5_id.tolist()[-1]:
				sub_df['away_player5']=game_subs_df.loc[ind].subbed_in_player_name
				sub_df['away_player5_id']=game_subs_df.loc[ind].subbed_in_player_id   
			else:
				if game_lineups_df.away_player1_id.tolist()[-1] == 0:
					sub_df['away_player1']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['away_player1_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.away_player2_id.tolist()[-1] == 0:
					sub_df['away_player2']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['away_player2_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.away_player3_id.tolist()[-1] == 0:
					sub_df['away_player3']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['away_player3_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.away_player4_id.tolist()[-1] == 0:
					sub_df['away_player4']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['away_player4_id']=game_subs_df.loc[ind].subbed_in_player_id
				if game_lineups_df.away_player5_id.tolist()[-1] == 0:
					sub_df['away_player5']=game_subs_df.loc[ind].subbed_in_player_name
					sub_df['away_player5_id']=game_subs_df.loc[ind].subbed_in_player_id
				
		
		sub_df['game_time']=game_subs_df.loc[ind]['game_time']
		sub_df['home_score']=game_subs_df.loc[ind]['home_score']
		sub_df['away_score']=game_subs_df.loc[ind]['away_score']
		sub_df['time_delta']=game_subs_df.loc[ind]['time_delta']
		sub_df['home_score_delta']=game_subs_df.loc[ind].home_score_delta
		sub_df['away_score_delta']=game_subs_df.loc[ind].away_score_delta
		sub_df['time_remaining']=game_subs_df.loc[ind].time_remaining
		
		game_lineups_df=game_lineups_df.append(sub_df).reset_index(drop=True)

	#Get time delta between subs
	game_lineups_df['time_delta'] = [game_lineups_df.game_time.diff(1)[1]]+game_lineups_df.time_delta[1:].tolist()

	#Establish column order/remove duplicate times
	col_order = ['game_id','home_team_abbr','away_team_abbr','home_player1','home_player1_id',
				'home_player2','home_player2_id','home_player3','home_player3_id','home_player4',
				'home_player4_id','home_player5','home_player5_id','away_player1','away_player1_id',
				'away_player2','away_player2_id','away_player3','away_player3_id','away_player4',
				'away_player4_id','away_player5','away_player5_id','home_score',
				'away_score','play','time','quarter','game_time','time_delta',
				'home_score_delta','away_score_delta','time_remaining']

	#game_lineups_df = game_lineups_df[game_lineups_df.time_delta > 0][col_order]
	game_lineups_df = game_lineups_df[col_order]

	#Save to staging lineups table
	game_lineups_df.to_sql('lineups_stage',
							con=engine,
							schema='nba_sandbox',
							if_exists='append',
							index=False,
							dtype={'game_id': sa.types.INTEGER(),
								   'home_team_abbr': sa.types.VARCHAR(length=255),
								   'away_team_abbr': sa.types.VARCHAR(length=255),
								   'home_player1': sa.types.VARCHAR(length=255),
								   'home_player1_id': sa.types.INTEGER(),
								   'home_player2': sa.types.VARCHAR(length=255),
								   'home_player2_id': sa.types.INTEGER(),
								   'home_player3': sa.types.VARCHAR(length=255),
								   'home_player3_id': sa.types.INTEGER(),
								   'home_player4': sa.types.VARCHAR(length=255),
								   'home_player4_id': sa.types.INTEGER(),
								   'home_player5': sa.types.VARCHAR(length=255),
								   'home_player5_id': sa.types.INTEGER(),
								   'away_player1': sa.types.VARCHAR(length=255),
								   'away_player1_id': sa.types.INTEGER(),
								   'away_player2': sa.types.VARCHAR(length=255),
								   'away_player2_id': sa.types.INTEGER(),
								   'away_player3': sa.types.VARCHAR(length=255),
								   'away_player3_id': sa.types.INTEGER(),
								   'away_player4': sa.types.VARCHAR(length=255),
								   'away_player4_id': sa.types.INTEGER(),
								   'away_player5': sa.types.VARCHAR(length=255),
								   'away_player5_id': sa.types.INTEGER(),
								   'home_score': sa.types.INTEGER(),
								   'away_score': sa.types.INTEGER(),
								   'play': sa.types.VARCHAR(length=255),
								   'time': sa.types.VARCHAR(length=255),
								   'quarter': sa.types.CHAR(length=3),
								   'game_time': sa.types.DECIMAL(),
								   'time_delta': sa.types.DECIMAL(),
								   'home_score_delta': sa.types.INTEGER(),
								   'away_score_delta': sa.types.INTEGER(),
								   'time_remaining': sa.types.DECIMAL()
								   })
		


def get_gameids(engine):

	query = '''
		select 
			gs.game_id
		from
			nba.game_summaries gs
		left join
			nba_sandbox.lineups_stage l 
			on gs.game_id=l.game_id
		left join
			nba.bad_gameids b
			on gs.game_id=b.game_id
			and b.table='lineups'
		where 1=1
			and gs.season >= 2018
			and gs.status='Final'
			and l.game_id is Null 
			and b.game_id is Null
	'''

	return pd.read_sql(query,engine)


#Writes lineups for all games
def write_lineups_stage(game_ids,engine,starters_qry,subs_qry):

	#print('# Games to process: ' + str(len(game_ids)))
	cnt=0
	cnt_success=0
	for game_id in game_ids.game_id.tolist():

		#Get starters and subs dataframes
		starters_df=get_starters(game_id,engine,starters_qry)
		subs_df=get_subs(game_id,engine,subs_qry)

		#Write lineups one game at a time
		try:
			write_game_lineup(starters_df,subs_df,engine)
			cnt_success+=1
		except:
			bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['lineups']})
			bad_gameid_df.to_sql('bad_gameids',
								  con=engine,
								  schema='nba',
								  index=False,
								  if_exists='append',
								  dtype={'game_id': sa.types.INTEGER(),
										 'table': sa.types.VARCHAR(length=255)})
		
		#if np.mod(cnt,20) == 0 and cnt > 1:
		#	print(str(round(cnt*100.0/len(game_ids),2)) + "% processed")
		
		cnt+=1

	print('Staging table processed')
	return cnt_success


#Write final lineups table to database
def write_full_lineups(game_ids,engine,lineups_qry,win_prob_qry,fp):

	try:
		game_id_list='('+functools.reduce(lambda a,b: str(a)+','+str(b),[x for x in game_ids.game_id])+')'
	except:
		game_id_list='('+str(game_ids.game_id.tolist()[0])+')'
	
	lineups_df=pd.read_sql(lineups_qry.format(game_id_list),engine)
	
	lineups_df['home_team_flg']=1
	lineups_df['away_team_flg']=0

	#deploy win probability model
	wp_model=pickle.load(open(fp+'/win_probability.sav', 'rb'))
	
	home_wp_results=np.array([x[1] for x in wp_model.predict_proba(lineups_df[['home_score_delta','home_team_flg','time_remaining']])])
	away_wp_results=np.array([x[1] for x in wp_model.predict_proba(lineups_df[['away_score_delta','away_team_flg','time_remaining']])])
	
	lineups_df['home_win_probability']=home_wp_results/(home_wp_results+away_wp_results)
	lineups_df['away_win_probability']=away_wp_results/(home_wp_results+away_wp_results)

	lineups_df['total_possessions']=(lineups_df['home_team_possessions']+lineups_df['away_team_possessions'])/2.0

	col_order=['game_id','home_team_abbr','away_team_abbr','home_player1','home_player1_id','home_player2','home_player2_id',
			   'home_player3','home_player3_id','home_player4','home_player4_id','home_player5','home_player5_id','away_player1',
			   'away_player1_id','away_player2','away_player2_id','away_player3','away_player3_id','away_player4','away_player4_id',
			   'away_player5','away_player5_id','home_score','away_score','time','quarter','game_time','time_delta','home_team_possessions',
			   'away_team_possessions','total_possessions','home_team_pts','away_team_pts','home_team_reb','away_team_reb','total_reb',
			   'home_team_fgm','home_team_fga','home_team_3pm','home_team_3pa','home_team_ast','away_team_fgm','away_team_fga','away_team_3pm',
			   'away_team_3pa','away_team_ast','home_team_stl','home_team_tov','home_team_fls','away_team_stl','away_team_tov','away_team_fls',
			   'home_win_probability','away_win_probability']

	print('Writing lineups table...')
	lineups_df[col_order].to_sql('lineups',
						con=engine,
						schema='nba',
						if_exists='append',
						index=False,
						dtype={'game_id': sa.types.INTEGER(),
								'home_team_abbr': sa.types.VARCHAR(length=255),
								'away_team_abbr': sa.types.VARCHAR(length=255),
								'home_player1': sa.types.VARCHAR(length=255),
								'home_player1_id': sa.types.INTEGER(),
								'home_player2': sa.types.VARCHAR(length=255),
								'home_player2_id': sa.types.INTEGER(),
								'home_player3': sa.types.VARCHAR(length=255),
								'home_player3_id': sa.types.INTEGER(),
								'home_player4': sa.types.VARCHAR(length=255),
								'home_player4_id': sa.types.INTEGER(),
								'home_player5': sa.types.VARCHAR(length=255),
								'home_player5_id': sa.types.INTEGER(),
								'away_player1': sa.types.VARCHAR(length=255),
								'away_player1_id': sa.types.INTEGER(),
								'away_player2': sa.types.VARCHAR(length=255),
								'away_player2_id': sa.types.INTEGER(),
								'away_player3': sa.types.VARCHAR(length=255),
								'away_player3_id': sa.types.INTEGER(),
								'away_player4': sa.types.VARCHAR(length=255),
								'away_player4_id': sa.types.INTEGER(),
								'away_player5': sa.types.VARCHAR(length=255),
								'away_player5_id': sa.types.INTEGER(),
								'home_score': sa.types.INTEGER(),
								'away_score': sa.types.INTEGER(),
								'time': sa.types.VARCHAR(length=255),
								'quarter': sa.types.CHAR(length=3),
								'game_time': sa.types.DECIMAL(),
								'time_delta': sa.types.DECIMAL(),
								'home_team_possessions': sa.types.DECIMAL(),
								'away_team_possessions': sa.types.DECIMAL(),
								'total_possessions': sa.types.DECIMAL(),
								'home_team_pts': sa.types.INTEGER(),
								'away_team_pts': sa.types.INTEGER(),
								'home_team_reb': sa.types.INTEGER(),
								'away_team_reb': sa.types.INTEGER(),
								'total_reb': sa.types.INTEGER(),
								'home_team_fgm': sa.types.INTEGER(),
								'home_team_fga': sa.types.INTEGER(),
								'home_team_3pm': sa.types.INTEGER(),
								'home_team_3pa': sa.types.INTEGER(),
								'home_team_ast': sa.types.INTEGER(),
								'away_team_fgm': sa.types.INTEGER(),
								'away_team_fga': sa.types.INTEGER(),
								'away_team_3pm': sa.types.INTEGER(),
								'away_team_3pa': sa.types.INTEGER(),
								'away_team_ast': sa.types.INTEGER(),
								'home_team_stl': sa.types.INTEGER(),
								'home_team_tov': sa.types.INTEGER(),
								'home_team_fls': sa.types.INTEGER(),
								'away_team_stl': sa.types.INTEGER(),
								'away_team_tov': sa.types.INTEGER(),
								'away_team_fls': sa.types.INTEGER(),
								'home_win_probability': sa.types.DECIMAL(),
								'away_win_probability': sa.types.DECIMAL()
								})


def main():

	#Get filepath where code is run
	fp=os.path.dirname(os.path.realpath(__file__))

	#Get sql queries for starters, subs and lineups
	starters_qry,subs_qry,lineups_qry,win_prob_qry=get_sql(fp)

	#Get engine
	engine=get_engine(fp)

	#Get game_ids to process
	game_ids=get_gameids(engine)

	#Write staging lineups to database
	num_written=write_lineups_stage(game_ids,engine,starters_qry,subs_qry)

	#Write final lineups table to database
	if num_written > 0:
		print(num_written)
		write_full_lineups(game_ids,engine,lineups_qry,win_prob_qry,fp)


if __name__ == "__main__":
	main() 


