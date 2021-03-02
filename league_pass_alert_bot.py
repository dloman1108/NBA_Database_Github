
from twitter import *
import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml
import emoji
import time

import datetime



token='1221106509003751426-Orridb7OJfxsCA1uzLZzyAjkKmf7A1'
token_secret='gqALmb3egq3yoTrI5bPcKhWpjv7JLQgaLz1pbMfquHkbz'

consumer_key='lmZk3qz7ogiQn7xgtmdCrvFgP'
consumer_secret='6tzaKuzPfKvzH0igMs9MZaNd92VmaXk9tAfkIvAd4YuY5OZxCc'



day=(datetime.date.today() - datetime.timedelta(days=1)).day
month=(datetime.date.today() - datetime.timedelta(days=1)).month
yesterday=str(month)+'/'+str(day)

t = Twitter(
    auth=OAuth(token, token_secret, consumer_key, consumer_secret))


active_games_query='''
select game_id from nba_sandbox.game_summaries_realtime
where status in ('In Progress','Halftime')
'''


lpa_query='''
select 
    game_id
    ,home_team_abbr
    ,home_team_score
    ,away_team_abbr
    ,away_team_score
    ,display_clock
    ,home_team
from 
    nba_sandbox.game_summaries_realtime
WHERE 1=1
    --and period >= 4 
	and status in ('In Progress')
    --and clock < 300
    and abs(home_team_score - away_team_score) <= 5
limit 1
'''


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

engine=get_engine()

fp=os.path.dirname(os.path.realpath(__file__))



exec(open(fp+'/Raw Data/GameSummariesRealtime.py').read())
active_games_df=pd.read_sql(active_games_query,engine)

game_ids=[]
while len(active_games_df) > 0:

    #League Pass Alert
    lpa_df=pd.read_sql(lpa_query,engine)

    if len(lpa_df) > 0:

        home_team_abbr=lpa_df.iloc[0].home_team_abbr
        home_team_score=lpa_df.iloc[0].home_team_score
        away_team_abbr=lpa_df.iloc[0].away_team_abbr
        away_team_score=lpa_df.iloc[0].away_team_score
        display_clock=lpa_df.iloc[0].display_clock

        lpa_string=emoji.emojize(':police_car_light:')+' #LeaguePassAlert '+emoji.emojize(':police_car_light:')+'\n\n'

        if home_team_score >= away_team_score:
            lpa_string+='{} {}, {} {} with {} remaining'.format(home_team_abbr,home_team_score,away_team_abbr,away_team_score,display_clock)
        else:
            lpa_string+='{} {}, {} {} with {} remaining'.format(away_team_abbr,away_team_score,home_team_abbr,home_team_score,display_clock)

        t.statuses.update(
            status=lpa_string)


        game_ids.append(str(lpa_df.iloc[0].game_id))
        lpa_query='''
            select 
                game_id
                ,home_team_abbr
                ,home_team_score
                ,away_team_abbr
                ,away_team_score
                ,display_clock
                ,home_team
            from 
                nba_sandbox.game_summaries_realtime
            WHERE 1=1
                --and period >= 4 
                and status in ('In Progress')
                --and clock < 300
                --and abs(home_team_score - away_team_score) <= 5
                --and game_id not in {}
            limit 1
            '''.format("("+",".join(game_ids)+")")

    time.sleep(60)

    exec(open(fp+'/Raw Data/GameSummariesRealtime.py').read())
    active_games_df=pd.read_sql(active_games_query,engine)

    
    