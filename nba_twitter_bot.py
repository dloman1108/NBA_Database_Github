# -*- coding: utf-8 -*-
"""
Created on Sat Jan 25 11:09:05 2020

@author: dh08loma
"""

from twitter import *
import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml
import emoji

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


leaders_query='''
select 
	pb.player
	,pb.team_abbr
	,pts
    ,cast(round(pts*100/(2*fga+0.44*fta),0) as int) ts
	,reb
    ,ast
	,stl
	,blk
	,tov
    ,fg
    ,fg3
    ,ft
from 
	nba.player_boxscores pb
join
	nba.game_summaries gs on pb.game_id=gs.game_id
join
	nba.player_reference pr on pb.player_id=pr.player_id
where
	gs.date = cast(CURRENT_DATE - INTERVAL '1 day' as date)
	and pb.dnp_reason is null
    and (fga+0.44*fta) > 0
order by
	pts + .2*reb + 1.7*stl + .535*blk + .5*ast - .9*fga - .35*fta - 1.4*tov desc
limit 5
'''


scrubs_query='''
select 
	pb.player
	,pb.team_abbr
	,pts
    ,cast(round(pts*100/(2*fga+0.44*fta),0) as int) ts
	,reb
    ,ast
	,stl
	,blk
	,tov
    ,fg
    ,fg3
    ,ft
from 
	nba.player_boxscores pb
join
	nba.game_summaries gs on pb.game_id=gs.game_id
join
	nba.player_reference pr on pb.player_id=pr.player_id
where
	gs.date = cast(CURRENT_DATE - INTERVAL '1 day' as date)
	and pb.dnp_reason is null
    and (fga+0.44*fta) > 0
order by
	pts + .2*reb + 1.7*stl + .535*blk + .5*ast - .9*fga - .35*fta - 1.4*tov 
limit 5
'''


rookies_query='''
select 
	pb.player
	,pb.team_abbr
	,pts
    ,cast(round(pts*100/(2*fga+0.44*fta),0) as int) ts
	,reb
    ,ast
	,stl
	,blk
	,tov
    ,fg
    ,fg3
    ,ft
from 
	nba.player_boxscores pb
join
	nba.game_summaries gs on pb.game_id=gs.game_id
join
	nba.player_reference pr on pb.player_id=pr.player_id
where
	gs.date = cast(CURRENT_DATE - INTERVAL '1 day' as date)
	and pb.dnp_reason is null
	and pr.experience='Rookie'
    and (fga+0.44*fta) > 0
order by
	pts + .2*reb + 1.7*stl + .535*blk + .5*ast - .9*fga - .35*fta - 1.4*tov desc
limit 5
'''


surprises_query='''
with yearly_dre as (
	select 
		pb.player_id
		,pb.player
		,avg(pts + .5*reb + stl + blk + .6*ast - .9*fga - .35*fta - 1.4*tov) avg_dre
		--,avg(pts + 0.4 * fgm - 0.7 * fga - 0.4*(fta - ftm) + 0.7 * oreb + 0.3 * dreb + stl + 0.7 * ast + 0.7 * blk - tov) avg_dre
	from 
		nba.player_boxscores pb
	join
		nba.game_summaries gs on pb.game_id=gs.game_id
	where
		gs.season=2021
		and gs.game_type='Regular Season'
		and pb.dnp_reason is null
	group by	
		pb.player
		,pb.player_id   
	having
		count(*) >= 10
)

, daily_dre as (
	select 
		pts + .5*reb + stl + blk + .6*ast - .9*fga - .35*fta - 1.4*tov avg_dre
		--pts + 0.4 * fgm - 0.7 * fga - 0.4*(fta - ftm) + 0.7 * oreb + 0.3 * dreb + stl + 0.7 * ast + 0.7 * blk - tov avg_dre
	from 
		nba.player_boxscores pb
	join
		nba.game_summaries gs on pb.game_id=gs.game_id
	where
		gs.season=2021
		and gs.game_type='Regular Season'
		and pb.dnp_reason is null 
		and mp > 0
)

--select * from daily_dre

--select * from yearly_dre where player='A. Simons'

, percentile_dre as (
select k, percentile_disc(k) within group (order by avg_dre)
from yearly_dre, generate_series(0.00, 1, 0.01) as k
group by k
)

, percentile_dre_lead as (
select k,"percentile_disc" p1,coalesce(lead("percentile_disc") over (partition by 1 order by percentile_disc),100) p2
,("percentile_disc" - 3.5168)/2.302 z
from percentile_dre
)

--select * from percentile_dre_lead

, percentile_dre_daily as (
select k, percentile_disc(k) within group (order by avg_dre)
from daily_dre, generate_series(0.00, 1, 0.01) as k
group by k
)

, percentile_dre_daily_lead as (
select k,"percentile_disc" p1,coalesce(lead("percentile_disc") over (partition by 1 order by percentile_disc),100) p2
,("percentile_disc" - 3.8255)/5.08 z
from percentile_dre_daily
)
--select * from percentile_dre_daily_lead

select 
	pb.player
	,pb.team_abbr
	,pts
    ,cast(round(pts*100/(2*fga+0.44*fta),0) as int) ts
	,reb
    ,ast
	,stl
	,blk
	,tov
    ,fg
    ,fg3
    ,ft
    ,pts + .5*reb + stl + blk + .6*ast - .9*fga - .35*fta - 1.4*tov
    --,pts + 0.4 * fgm - 0.7 * fga - 0.4*(fta - ftm) + 0.7 * oreb + 0.3 * dreb + stl + 0.7 * ast + 0.7 * blk - tov
    ,yd.avg_dre
    ,perc_daily.*
    ,perc_yearly.*
from 
	nba.player_boxscores pb
join
	nba.game_summaries gs on pb.game_id=gs.game_id
join
	yearly_dre yd on pb.player_id=yd.player_id
join
	percentile_dre_daily_lead perc_daily
	on pts + .5*reb + stl + blk + .6*ast - .9*fga - .35*fta - 1.4*tov > perc_daily.p1 
	and pts + .5*reb + stl + blk + .6*ast - .9*fga - .35*fta - 1.4*tov <= perc_daily.p2 
	--on pts + 0.4 * fgm - 0.7 * fga - 0.4*(fta - ftm) + 0.7 * oreb + 0.3 * dreb + stl + 0.7 * ast + 0.7 * blk - tov between perc_daily.p1 and perc_daily.p2
join
	percentile_dre_lead perc_yearly
	on yd.avg_dre > perc_yearly.p1 and yd.avg_dre <= perc_yearly.p2
where
	gs.date = cast(CURRENT_DATE - INTERVAL '1 day' as date)
	and pb.dnp_reason is null
    and (fga+0.44*fta) > 0
    and perc_daily.k > .5
    and perc_yearly.k < .5
order by
	perc_daily.k-perc_yearly.k desc
limit 5


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


#Daily Leaders
daily_leaders_df=pd.read_sql(leaders_query,engine)

daily_str=yesterday+' Leaders '+emoji.emojize(':fire:')+':\n\n'
for ind in daily_leaders_df.index.values:
    x=daily_leaders_df.iloc[ind]
    if ind == 0:
        daily_str+=x.player+' - '+str(x.pts)+' pts ('+str(x.ts)+'%ts)/'+str(x.reb)+' reb/'+str(x.ast)+' ast/'+str(x.stl)+' stl/'+str(x.blk)+' blk/'+str(x.tov)+' tov'    
    else:
        daily_str+=x.player+' - '+str(x.pts)+' ('+str(x.ts)+'%)/'+str(x.reb)+'/'+str(x.ast)+'/'+str(x.stl)+'/'+str(x.blk)+'/'+str(x.tov)       
    daily_str+='\n'


t.statuses.update(
    status=daily_str)



#Daily Scrubs
daily_scrubs_df=pd.read_sql(scrubs_query,engine)

scrubs_str=yesterday+' Scrubs '+emoji.emojize(':face_vomiting:')+':\n\n'
for ind in daily_scrubs_df.index.values:
    x=daily_scrubs_df.iloc[ind]
    if ind == 0:
        scrubs_str+=x.player+' - '+str(x.pts)+' pts ('+str(x.ts)+'%ts)/'+str(x.reb)+' reb/'+str(x.ast)+' ast/'+str(x.stl)+' stl/'+str(x.blk)+' blk/'+str(x.tov)+' tov'    
    else:
        scrubs_str+=x.player+' - '+str(x.pts)+' ('+str(x.ts)+'%)/'+str(x.reb)+'/'+str(x.ast)+'/'+str(x.stl)+'/'+str(x.blk)+'/'+str(x.tov)       
    scrubs_str+='\n'


t.statuses.update(
    status=scrubs_str)


#Rookie Leaders
rookie_leaders_df=pd.read_sql(rookies_query,engine)

rookie_str=yesterday+' Rookie Leaders '+emoji.emojize(':baby:')+':\n\n'
for ind in rookie_leaders_df.index.values:
    x=rookie_leaders_df.iloc[ind]
    if ind == 0:
        rookie_str+=x.player+' - '+str(x.pts)+' pts ('+str(x.ts)+'%ts)/'+str(x.reb)+' reb/'+str(x.ast)+' ast/'+str(x.stl)+' stl/'+str(x.blk)+' blk/'+str(x.tov)+' tov'    
    else:
        rookie_str+=x.player+' - '+str(x.pts)+' ('+str(x.ts)+'%)/'+str(x.reb)+'/'+str(x.ast)+'/'+str(x.stl)+'/'+str(x.blk)+'/'+str(x.tov)       
    rookie_str+='\n'


t.statuses.update(
    status=rookie_str)





#Surprises
surprises_df=pd.read_sql(surprises_query,engine)

surprises_str=yesterday+' Surprises '+emoji.emojize(':eyes:')+':\n\n'
for ind in surprises_df.index.values:
    x=surprises_df.iloc[ind]
    if ind == 0:
        surprises_str+=x.player+' - '+str(x.pts)+' pts ('+str(x.ts)+'%ts)/'+str(x.reb)+' reb/'+str(x.ast)+' ast/'+str(x.stl)+' stl/'+str(x.blk)+' blk/'+str(x.tov)+' tov'    
    else:
        surprises_str+=x.player+' - '+str(x.pts)+' ('+str(x.ts)+'%)/'+str(x.reb)+'/'+str(x.ast)+'/'+str(x.stl)+'/'+str(x.blk)+'/'+str(x.tov)       
    surprises_str+='\n'


t.statuses.update(
    status=surprises_str)

