# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 16:45:38 2016

@author: DanLo1108
"""

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re
import sklearn
import string as st
import matplotlib.pyplot as plt
import os
import string as st

import sqlalchemy as sa




def get_shot_type(x):
    if 'three point' in x.Text:
        return 3
    else:
        return 2

def get_shot_area(x):
    if x.ShotDistance <= 4:
        return 'RA'
    elif (x.YPos >= 17 and x.YPos <= 33) and x.XPos <= 19:
        return 'Paint'
    elif x.ShotType == 3 and x.XPos <= 14:
        return 'Crnr3'
    elif x.ShotType == 3 and x.XPos > 14:
        return 'AbvBrk3'
    else:
        return 'MidRng'
    
    
def get_shot_distance_class(x):
        if x.ShotDistance < 3:
        return '0-3ft'
    elif x.ShotDistance >= 3 and x.ShotDistance < 10:
        return '3-10ft'
    elif x.ShotDistance >= 10 and x.ShotDistance < 16:
        return '10-16ft'
    elif x.ShotDistance >= 16 and x.ShotType == 2:
        return '16-3pt'
    elif x.ShotType == 3:
        return '3pt'



def append_shot_chart(game_id,engine):
     
    url='http://www.espn.com/nba/playbyplay?gameId='+str(game_id)

    page = urlopen(url)
    
    
    content=page.read()
    soup=BeautifulSoup(content,'lxml')
    
    li=soup.find_all('li')
        
    #Add gameid
    shot_chart=[]
    for l in li:
        try:
            classs=l['class'][0]
            homeaway=l['data-homeaway']
            period=l['data-period']
            shooter_id=l['data-shooter']
            text=l['data-text']
            shot_id=l['id']
            left=round(float(l['style'][l['style'].index('left:')+5:l['style'].index('%')]),2)
            top=round(float(l['style'][l['style'].index('top:')+4:-2]),2)
            
            if left*(94./100) < 47:
                x_pos=left*(94./100)
            else:
                x_pos=94-left*(94./100)
            
            if x_pos <= 47:
                y_pos=top*(50./100)
            else:
                y_pos=50-top*(50./100)
                
            distance=np.sqrt((x_pos-5.25)**2+(y_pos-25)**2)
            angle=np.arctan(np.abs(y_pos-25)/(x_pos-5.25))
            shot_chart.append((int(game_id),classs,homeaway,period,shooter_id,text,shot_id,left,top,x_pos,y_pos,distance,angle))
        except:
            continue
        
        
    shot_chart_df=pd.DataFrame(shot_chart,columns=['GameID','Class','HomeAway','Quarter','ShooterID',
                                           'Text','ShotID','Left','Top','XPos','YPos','ShotDistance','ShotAngle'])


    shot_chart_df['ShotType']=shot_chart_df.apply(lambda x: get_shot_type(x),axis=1)
    shot_chart_df['ShotArea']=shot_chart_df.apply(lambda x: get_shot_area(x),axis=1)
    shot_chart_df['ShotDistanceClass']=shot_chart_df.apply(lambda x: get_shot_distance_class(x),axis=1)
    
    column_order=['GameID', 'ShotID', 'Class', 'HomeAway', 'Quarter', 'ShooterID', 'Text',
                  'Left', 'Top', 'XPos', 'YPos', 'ShotDistance', 'ShotAngle', 'ShotType',
                  'ShotArea', 'ShotDistanceClass']
    
    shot_chart_df[column_order].to_sql('shot_chart',
                         con=engine,
                         schema='nba',
                         index=False,
                         if_exists='append',
                         dtype={'GameID': sa.types.INTEGER(),
                                'ShotID': sa.types.INTEGER(),
                                'Class': sa.types.VARCHAR(length=255),
                                'HomeAway': sa.types.CHAR(length=4),
                                'Quarter': sa.types.INTEGER(),
                                'ShooterID': sa.types.INTEGER(),
                                'Text': sa.types.VARCHAR(length=255),
                                'Left': sa.types.FLOAT(),
                                'Top': sa.types.FLOAT(),
                                'XPos': sa.types.FLOAT(),
                                'YPos': sa.types.FLOAT(),
                                'ShotDistance': sa.types.FLOAT(),
                                'ShotAngle': sa.types.FLOAT(),
                                'ShotType': sa.types.INTEGER(),
                                'ShotArea': sa.types.VARCHAR(length=255),
                                'ShotDistanceClass': sa.types.VARCHAR(length=255)})      
    



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
    
    db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(username,password,endpoint,port,database)
    engine=sa.create_engine(db_string)
    
    return engine

def get_gameids(engine):
    game_id_query='''
    select distinct
        gs."Season"
        ,gs."GameID"
    from
        nba.game_summaries gs
    left join
        nba.shot_chart p on cast(gs."GameID" as integer)=cast(p."GameID" as integer) 
    where
        p."GameID" is Null
        and gs."Season"=(select max("Season") from nba.game_summaries)
    order by
        gs."Season"
    '''
    
    game_ids=pd.read_sql(game_id_query,engine)
    
    return game_ids.GameID.tolist()

            
def update_team_boxscores(engine,game_id_list):
    cnt=0
    bad_gameids=[]
    for game_id in game_id_list:
        
        if np.mod(cnt,2000)==0:
            print('CHECK: ',cnt,len(bad_gameids))
    
        try:
            append_shot_chart(game_id,engine)
            cnt+=1
            if np.mod(cnt,100)==0:
                print(str(round(float(cnt*100.0/len(game_ids)),2))+'%')
            
        except:
            bad_gameids.append(game_id)
            cnt+=1
            if np.mod(cnt,100) == 0:
                print(str(round(float(cnt*100.0/len(game_ids)),2))+'%')
            continue
        
        
def main():
    engine=get_engine()
    game_ids=get_dates(engine)
    update_team_boxscores(engine,game_ids)
    
    
    
if __name__ == "__main__":
    main()
