# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:50:53 2016

@author: DanLo1108
"""

#'End Game' not in list?


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import os
import yaml
import string as st
import matplotlib.pyplot as plt
import os
import sqlalchemy as sa



### FUNCTIONS ###

#Notes - cannot get old teams (SEA,NJN) so instead, link to box scores
#on gameid and playerid
#Write function for PlayerID
#Get home score and away score, join on game summaries to get team scores

def get_Team(x,ht,at):
    try:
        a=x.index(st.lower(ht)+'.')
        return ht
    except:
        try:
            a=x.index(st.lower(at))
            return at
        except:
            return None


def get_HomeScore(x,ht,at):
    return int(x.Score[x.Score.index('-')+1:])
        
def get_AwayScore(x,ht,at):
    return int(x.Score[:x.Score.index('-')-1])
        
def get_Quarter(x,pbp_df):
    if 'End of the 1st Quarter' in pbp_df.Play.tolist():
        q1_ind=pbp_df.Play.tolist().index('End of the 1st Quarter')
    elif 'End of the 1st Quarter.' in pbp_df.Play.tolist():
        q1_ind=pbp_df.Play.tolist().index('End of the 1st Quarter.')
    elif 'Start of the 2nd Quarter' in pbp_df.Play.tolist():
        q1_ind=pbp_df.Play.tolist().index('Start of the 2nd Quarter')
    elif 'Start of the 2nd Quarter.' in pbp_df.Play.tolist():
        q1_ind=pbp_df.Play.tolist().index('Start of the 2nd Quarter.')
    else:
        q1_ind=-1
        
    if 'End of the 2nd Quarter' in pbp_df.Play.tolist():
        q2_ind=pbp_df.Play.tolist().index('End of the 2nd Quarter')
    elif 'End of the 2nd Quarter.' in pbp_df.Play.tolist():
        q2_ind=pbp_df.Play.tolist().index('End of the 2nd Quarter.')
    elif 'Start of the 3rd Quarter' in pbp_df.Play.tolist():
        q2_ind=pbp_df.Play.tolist().index('Start of the 3rd Quarter')
    elif 'Start of the 3rd Quarter.' in pbp_df.Play.tolist():
        q2_ind=pbp_df.Play.tolist().index('Start of the 3rd Quarter.')
    else:
        q2_ind=-1
        
    if 'End of the 3rd Quarter' in pbp_df.Play.tolist():
        q3_ind=pbp_df.Play.tolist().index('End of the 3rd Quarter')
    elif 'End of the 3rd Quarter.' in pbp_df.Play.tolist():
        q3_ind=pbp_df.Play.tolist().index('End of the 3rd Quarter.')
    elif 'Start of the 4th Quarter' in pbp_df.Play.tolist():
        q3_ind=pbp_df.Play.tolist().index('Start of the 4th Quarter')
    elif 'Start of the 4th Quarter.' in pbp_df.Play.tolist():
        q3_ind=pbp_df.Play.tolist().index('Start of the 4th Quarter.')
    else:
        q3_ind=-1
        
    if 'End of the 4th Quarter' in pbp_df.Play.tolist():
        q4_ind=pbp_df.Play.tolist().index('End of the 4th Quarter')
    elif 'End of the 4th Quarter.' in pbp_df.Play.tolist():
        q4_ind=pbp_df.Play.tolist().index('End of the 4th Quarter.')
    elif 'End of the Game' in pbp_df.Play.tolist():
        q4_ind=pbp_df.Play.tolist().index('End of the Game')
    elif 'Start of the 2nd Quarter.' in pbp_df.Play.tolist():
        q4_ind=pbp_df.Play.tolist().index('Start of the 2nd Quarter.')
    else:
        q4_ind=-1
        
    if 'End of Game' in pbp_df.Play.tolist():
        ge_ind=pbp_df.Play.tolist().index('End of Game')
    elif 'End of the Game' in pbp_df.Play.tolist():
        ge_ind=pbp_df.Play.tolist().index('End of the Game')
    else:
        ge_ind=-1
        
    try:
        ot1_ind=pbp_df.Play.tolist().index('End of the 1st  Overtime')
    except:
        try:
            ot1_ind=pbp_df.Play.tolist().index('End of the 1st  Overtime.')
        except:
            ot1_ind=-1
    try:
        ot2_ind=pbp_df.Play.tolist().index('End of the 2nd  Overtime')
    except:
        try:
            ot2_ind=pbp_df.Play.tolist().index('End of the 2nd  Overtime.')
        except:
            ot2_ind=-1
    try:
        ot3_ind=pbp_df.Play.tolist().index('End of the 3rd  Overtime')
    except:
        try:
            ot3_ind=pbp_df.Play.tolist().index('End of the 3rd  Overtime.')
        except:
            ot3_ind=-1
    try:
        ot4_ind=pbp_df.Play.tolist().index('End of the 4th  Overtime')
    except:
        try:
            ot4_ind=pbp_df.Play.tolist().index('End of the 4th  Overtime.')
        except:
            ot4_ind=-1
    try:
        ot5_ind=pbp_df.Play.tolist().index('End of the 5th  Overtime')
    except:
        try:
            ot5_ind=pbp_df.Play.tolist().index('End of the 5th  Overtime.')
        except:
            ot5_ind=-1
            
    if x.name <= q1_ind:
        return '1'
    elif x.name <= q2_ind:
        return '2'
    elif x.name <= q3_ind:
        return '3'
    elif x.name <= q4_ind:
        return '4'
    elif x.name <= ot1_ind:
        return 'OT1'
    elif x.name <= ot2_ind:
        return 'OT2'
    elif x.name <= ot3_ind:
        return 'OT3'
    elif x.name <= ot4_ind:
        return 'OT4'
    elif x.name <= ot5_ind:
        return 'OT5'
    elif x.name == ge_ind:
        return None
    else:
        return None
    

def get_Player(x):
    keywords=['shooting','misses','offensive',
          'defensive','makes','loose',"'s",'enters','20 Sec',
          'Full','lost ball','personal','bad','turnover','delay',
          'out','flagrant','technical','kicked']
    for kw in keywords:
        try:
            if kw=="'s":
                player=x.Play[x.Play.index('blocks')+7:x.Play.index(kw)].strip()
            else:
                player=x.Play[:x.Play.index(kw)].strip()
            if any(x.isupper() for x in player):
                return player
        except:
            continue
        
#Classify play type based on text               
def get_PlayType(x):
    play=x.Play.lower()
    if 'vs.' in play:
        return 'Jump Ball'
    if 'makes' in play and 'free throw' not in play:
        return 'Made Field Goal'
    if ('misses' in play or 'blocks' in play) and 'free throw' not in play:
        return 'Missed Field Goal'
    if 'makes' in play and 'free throw' in play:
        return 'Made Free Throw'
    if 'misses' in play and 'free throw' in play:
        return 'Missed Free Throw'
    if 'rebound' in play and 'offensive' in play:
        return 'Offensive Rebound'
    if 'rebound' in play and 'defensive' in play:
        return 'Defensive Rebound'
    if 'enters' in play:
        return 'Substitution'
    if 'timeout' in play:
        return 'Timeout'
    if 'turnover' in play or 'bad pass' in play \
    or 'travel' in play or 'lost ball' in play \
    or 'backcourt' in play or 'shot clock violation' in play \
    or 'discontinue dribble' in play or 'double dribble' in play \
    or 'charge' in play or ('foul' in play and 'offensive' in play):
        return 'Turnover'
    if '3 second' in play or 'illegal defense' in play:
        return '3 second/illegal defense'
    if 'foul' in play and 'offensive' not in play:
        return 'Foul'
    if 'kicked ball' in play:
        return 'Kicked Ball'
    
    
#Get player responsible for event        
def get_Assistor(x):
    if 'assists' in x.Play:
        return x.Play[x.Play.index('(')+1:x.Play.index('assists')].strip()
         
         
def get_StolenBy(x):
    try:
        if 'steals' in x.Play:
            return x.Play[x.Play.index('(')+1:x.Play.index('steals')].strip()
    except:
        return None
            
def get_BlockedBy(x):
    if 'blocks' in x.Play:
        return x.Play[:x.Play.index('blocks')].strip()
        
def get_Points(x,df):
    if 'makes' in x.Play:
        if 'free throw' in x.Play:
            return 1
        elif 'three point' in x.Play:
            return 3
        else:
            return 2
    else:
        return 0
        

def get_SubbedIn(x):
    if 'enters' in x.Play:
        return x.Play[:x.Play.index('enters')].strip()


def get_SubbedOut(x):
    try:
        if x.Play=='JR Smith enters the game for':
            return 'Kyle Korver'
        if 'enters' in x.Play:
            return x.Play[x.Play.index('game for ')+9:]
    except:
        return None




def append_pbp(game_id,engine):
    
    url='http://www.espn.com/nba/playbyplay?gameId='+str(game_id)
    
    page = urlopen(url)
    
    content=page.read()
    soup=BeautifulSoup(content,'lxml')  
    
    away_team,home_team=[team.string for team in soup.findAll('span', attrs={'class':'abbrev'})]
    
    pbp_df=pd.DataFrame({'Time':np.array([res.string for res in soup.find_all('td',attrs={'class','time-stamp'})]),
                         'Play':np.array([res.string for res in soup.find_all('td',attrs={'class','game-details'})]),
                         'Score':np.array([res.string for res in soup.find_all('td',attrs={'class','combined-score'})])})
    
    pbp_df['GameID']=[game_id]*len(pbp_df)
    #PlayByPlay['Date']=[x.Date]*len(PlayByPlay)
    
    if len(pbp_df) > 0:
        try:
            pbp_df['Team']=[get_Team(res['src'],home_team,away_team) for res in soup.find_all('img',attrs={'class','team-logo'})][len(pbp_df)*-1:]
        except:
            pbp_df['Team']=None
        pbp_df['HomeScore']=pbp_df.apply(lambda x: get_HomeScore(x,home_team,away_team),axis=1)
        pbp_df['AwayScore']=pbp_df.apply(lambda x: get_AwayScore(x,home_team,away_team),axis=1)
        pbp_df['Quarter']=pbp_df.apply(lambda x: get_Quarter(x,pbp_df),axis=1)
        pbp_df['Player']=pbp_df.apply(lambda x: get_Player(x),axis=1)
        #pbp_df['PlayerAbbr']=map(lambda x: x)
        pbp_df['PlayType']=pbp_df.apply(lambda x: get_PlayType(x),axis=1)   
        pbp_df['Points']=pbp_df.apply(lambda x: get_Points(x,pbp_df),axis=1)    
        pbp_df['Assistor']=pbp_df.apply(lambda x: get_Assistor(x),axis=1)
        pbp_df['StolenBy']=pbp_df.apply(lambda x: get_StolenBy(x),axis=1)   
        pbp_df['BlockedBy']=pbp_df.apply(lambda x: get_BlockedBy(x),axis=1) 
        pbp_df['SubbedIn']=pbp_df.apply(lambda x: get_SubbedIn(x),axis=1)   
        pbp_df['SubbedOut']=pbp_df.apply(lambda x: get_SubbedOut(x),axis=1) 
    else:
        pbp_df['Team']=[]
        pbp_df['TeamScore']=[]
        pbp_df['OppTeamScore']=[]
        pbp_df['Quarter']=[]
        pbp_df['Player']=[]
        pbp_df['PlayType']=[]
        pbp_df['Points']=[]
        pbp_df['Assistor']=[]
        pbp_df['StolenBy']=[]
        pbp_df['BlockedBy']=[]
        pbp_df['SubbedIn']=[]
        pbp_df['SubbedOut']=[]
    
    column_order=['GameID','Play', 'Score', 'Time', 'Team', 'HomeScore',
                  'AwayScore', 'Quarter', 'Player', 'PlayType', 'Points',
                  'Assistor', 'StolenBy', 'BlockedBy', 'SubbedIn', 'SubbedOut']
    
    
    pbp_df[column_order].to_sql('play_by_play',
                                con=engine,
                                schema='nba',
                                index=False,
                                if_exists='append',
                                dtype={'GameID': sa.types.INTEGER(),
                                       'Play': sa.types.VARCHAR(length=255),
                                       'Score': sa.types.VARCHAR(length=255),
                                       'Time': sa.types.TIME(),
                                       'Team': sa.types.VARCHAR(length=255),
                                       'HomeScore': sa.types.INTEGER(),
                                       
                                       'AwayScore': sa.types.INTEGER(),
                                       'Quarter': sa.types.INTEGER(),
                                       'Player': sa.types.VARCHAR(length=255),
                                       'PlayType': sa.types.VARCHAR(length=255),
                                       'Points': sa.types.INTEGER(),
                                       
                                       'Assistor': sa.types.VARCHAR(length=255),
                                       'StolenBy': sa.types.VARCHAR(length=255),
                                       'BlockedBy': sa.types.VARCHAR(length=255),
                                       'SubbedIn': sa.types.VARCHAR(length=255),
                                       'SubbedOut': sa.types.VARCHAR(length=255)})
           
#how to handle delayed game?          
    


#Get credentials stored in sql.yaml file (saved in root directory)
def get_engine()
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



def get_gameids(engine):
    
    game_id_query='''
    select distinct
        gs."Season"
        ,gs."GameID"
    from
        nba.game_summaries gs
    left join
        nba.play_by_play p on gs."GameID"=p."GameID" 
    where
        p."GameID" is Null
        and gs."Status"='Final'
        and gs."Season"=(select max("Season") from nba.game_summaries)
    order by
        gs."Season"
    '''
    
    game_ids=pd.read_sql(game_id_query,engine)
    
    return game_ids.GameID.tolist()

    
def update_play_by_play(engine,game_id_list):   
    cnt=0
    bad_gameids=[]
    for game_id in game_id_list:
    
        try:
            append_pbp(game_id,engine)
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
    game_ids=get_gameids(engine)
    update_play_by_play(engine,game_ids)
    
    
    
if __name__ == "__main__":
    main()       
        

