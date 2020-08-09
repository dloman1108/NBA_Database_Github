#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 16:20:03 2019

@author: dh08loma
"""


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import os
import yaml
import sqlalchemy as sa
import re


def get_engine():
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


def parse_draft_info(x):
    year=int(x[:4])
    rnd=int(x[x.index('Rd ')+3:x.index(',')])
    pick=int(x[x.index('Pk ')+3:x.index(' (')])
    team=x[x.index('(')+1:x.index(')')]
    
    return year,rnd,pick,team


def get_player_reference(player_ids,engine):
    
    player_reference_dict={
            'player_id':[],
            'player_name':[],
            'first_name':[],
            'last_name':[],
            'dob':[],
            'age':[],
            'experience':[],
            'birth_place':[],
            'current_team':[],
            'current_team_abbr':[],
            'current_team_id':[],
            'position':[],
            'position_abbr':[],
            'jersey_number':[],
            'height':[],
            'height_inches':[],
            'weight_lbs':[],
            'draft_year':[],
            'draft_round':[],
            'draft_pick':[],
            'draft_team':[],
            'ncaa_team':[],
            'ncaa_team_id':[],
            'ncaa_player_id':[],
            'active_status':[],
            'image_link':[]}
    
    cnt=0
    for player_id in player_ids:

        url='https://www.espn.com/nba/player/_/id/{}/'.format(str(player_id))
        
        page = urlopen(url)
        
        content=page.read()
        soup=BeautifulSoup(content,'lxml')  
        
        
        #Get scripts
        scripts=soup.find_all('script')
        
        #Get results from scripts
        results=[script.contents[0] for script in scripts if len(script.contents) > 0 and '{"athlete"' in script.contents[0]][0]
        
        #Parse results to dictonary readable
        results=results[results.index('{"app"'):-1]
        results=re.sub('false','False',results)
        results=re.sub('true','True',results)
        results=re.sub('null',"'Null'",results)
        
        #Get player results into dictionary
        e=eval(results)
        player_info=e['page']['content']['player']['prtlCmnApiRsp']['athlete']
    
        player_id=player_info['playerId']
        display_name=e['page']['content']['player']['plyrHdr']['ath']['dspNm']
        first_name=e['page']['content']['player']['plyrHdr']['ath']['fNm']
        try:
            last_name=e['page']['content']['player']['plyrHdr']['ath']['lNm']
        except:
            last_name=None
        try:
            dob=player_info['displayDOB']
            age=player_info['age']
        except:
            dob,age=[None,None]
        try:
            experience=player_info['displayExperience']
        except:
            experience=None
        try:
            birth_place=player_info['displayBirthPlace']
        except:
            birth_place=None
        try:
            current_team=player_info['team']['displayName']
            current_team_abbr=player_info['team']['abbrev']
            current_team_id=player_info['team']['id']
        except:
            current_team,current_team_abbr,current_team_id=[None,None,None]
        position=player_info['position']['displayName']
        try:
            position_abbr=e['page']['content']['player']['plyrHdr']['ath']['posAbv']
        except:
            position_abbr=None
        try:
            number=int(e['page']['content']['player']['plyrHdr']['ath']['dspNum'][1:])
        except:
            number=None
        try:
            height=player_info['displayHeight']
            height_inches=int(height[0])*12+int(height[height.index("\'")+2:-1])
        except:
            height,height_inches=[None,None]
        try:
            weight_lbs=int(player_info['displayWeight'][:-4])
        except:
            weight_lbs=None
        try:
            draft_year,draft_round,draft_pick,draft_team=parse_draft_info(player_info['displayDraft'])
        except:
            draft_year,draft_round,draft_pick,draft_team=[None,None,None,None]
        try:
            ncaa_team=player_info['college']['name']
            ncaa_team_id=player_info['college']['id']
            ncaa_player_id=player_info['collegeAthlete']['id']
        except:
            ncaa_team_id,ncaa_team,ncaa_player_id=[None,None,None]
        active_flg=player_info['status']['name']
        try:
            img=e['page']['content']['player']['plyrHdr']['ath']['img']
        except:
            img=None
        
        player_reference_dict['player_id'].append(player_id)
        player_reference_dict['player_name'].append(display_name)
        player_reference_dict['first_name'].append(first_name)
        player_reference_dict['last_name'].append(last_name)
        player_reference_dict['dob'].append(dob)
        player_reference_dict['age'].append(age)
        player_reference_dict['experience'].append(experience)
        player_reference_dict['birth_place'].append(birth_place)
        player_reference_dict['current_team'].append(current_team)
        player_reference_dict['current_team_abbr'].append(current_team_abbr)
        player_reference_dict['current_team_id'].append(current_team_id)
        player_reference_dict['position'].append(position)
        player_reference_dict['position_abbr'].append(position_abbr)
        player_reference_dict['jersey_number'].append(number)
        player_reference_dict['height'].append(height)
        player_reference_dict['height_inches'].append(height_inches)
        player_reference_dict['weight_lbs'].append(weight_lbs)
        player_reference_dict['draft_year'].append(draft_year)
        player_reference_dict['draft_round'].append(draft_round)
        player_reference_dict['draft_pick'].append(draft_pick)
        player_reference_dict['draft_team'].append(draft_team)
        player_reference_dict['ncaa_team'].append(ncaa_team)
        player_reference_dict['ncaa_team_id'].append(ncaa_team_id)
        player_reference_dict['ncaa_player_id'].append(ncaa_player_id)
        player_reference_dict['active_status'].append(active_flg)
        player_reference_dict['image_link'].append(img)
        
        cnt+=1
        if np.mod(cnt,100)==0:
            print(str(round(float(cnt*100.0/len(player_ids)),2))+'%')
        
        
    player_reference_df=pd.DataFrame(player_reference_dict)
    
    player_reference_df.to_sql('player_reference',
                               con=engine,
                               schema='nba',
                               index=False,
                               if_exists='replace',
                               dtype={'player_id': sa.types.INTEGER(),
                                      'player_name': sa.types.VARCHAR(length=255),
                                      'first_name': sa.types.VARCHAR(length=255),
                                      'last_name': sa.types.VARCHAR(length=255),
                                      'dob': sa.types.DATE(),
                                      'age': sa.types.INTEGER(),
                                      'experience': sa.types.VARCHAR(length=255),
                                      'birth_place': sa.types.VARCHAR(length=255),
                                      'current_team': sa.types.VARCHAR(length=255),
                                      'current_team_abbr': sa.types.CHAR(length=5),
                                      'current_team_id': sa.types.INTEGER(),
                                      'position': sa.types.VARCHAR(length=255),
                                      'position_abbr': sa.types.CHAR(length=5),
                                      'jersey_number': sa.types.CHAR(length=5),
                                      'height': sa.types.CHAR(6),
                                      'height_inches': sa.types.INTEGER(),
                                      'weight_lbs': sa.types.INTEGER(),
                                      'draft_year': sa.types.INTEGER(),
                                      'draft_round': sa.types.INTEGER(),
                                      'draft_pick': sa.types.INTEGER(),
                                      'draft_team': sa.types.CHAR(4),
                                      'ncaa_team': sa.types.VARCHAR(length=255),
                                      'ncaa_team_id': sa.types.INTEGER(),
                                      'ncaa_player_id': sa.types.INTEGER(),
                                      'active_status': sa.types.CHAR(8),
                                      'image_link': sa.types.VARCHAR(length=255),
                                       })
    
        


def get_player_ids(engine):   

    player_id_query='select distinct player_id from nba.player_boxscores'
    player_ids=pd.read_sql(player_id_query,engine)
    
    return player_ids.player_id.tolist()



def main():
    engine=get_engine()
    player_ids=get_player_ids(engine)
    get_player_reference(player_ids,engine)


if __name__ == "__main__":
    main()   

