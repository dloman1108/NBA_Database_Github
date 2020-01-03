# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:50:53 2016

@author: DanLo1108
"""


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import os
import yaml
import string as st
import os
import sqlalchemy as sa



### FUNCTIONS ###

#Notes - cannot get old teams (SEA,NJN) so instead, link to box scores
#on gameid and playerid
#Write function for PlayerID
#Get home score and away score, join on game summaries to get team scores

def get_Team(x,ht,at):
    try:
        a=x.index(ht.lower()+'.')
        return ht
    except:
        try:
            a=x.index(at.lower()+'.')
            return at
        except:
            return None


def get_HomeScore(x,ht,at):
    return int(x.score[x.score.index('-')+1:])
        
def get_AwayScore(x,ht,at):
    return int(x.score[:x.score.index('-')-1])
        

def get_Quarter(x,pbp_df):
    if 'End of the 1st Quarter' in pbp_df.play.tolist():
        q1_ind=pbp_df.play.tolist().index('End of the 1st Quarter')
    elif 'End of the 1st Quarter.' in pbp_df.play.tolist():
        q1_ind=pbp_df.play.tolist().index('End of the 1st Quarter.')
    elif 'Start of the 2nd Quarter' in pbp_df.play.tolist():
        q1_ind=pbp_df.play.tolist().index('Start of the 2nd Quarter')
    elif 'Start of the 2nd Quarter.' in pbp_df.play.tolist():
        q1_ind=pbp_df.play.tolist().index('Start of the 2nd Quarter.')
    else:
        q1_ind=-1
        
    if 'End of the 2nd Quarter' in pbp_df.play.tolist():
        q2_ind=pbp_df.play.tolist().index('End of the 2nd Quarter')
    elif 'End of the 2nd Quarter.' in pbp_df.play.tolist():
        q2_ind=pbp_df.play.tolist().index('End of the 2nd Quarter.')
    elif 'Start of the 3rd Quarter' in pbp_df.play.tolist():
        q2_ind=pbp_df.play.tolist().index('Start of the 3rd Quarter')
    elif 'Start of the 3rd Quarter.' in pbp_df.play.tolist():
        q2_ind=pbp_df.play.tolist().index('Start of the 3rd Quarter.')
    else:
        q2_ind=-1
        
    if 'End of the 3rd Quarter' in pbp_df.play.tolist():
        q3_ind=pbp_df.play.tolist().index('End of the 3rd Quarter')
    elif 'End of the 3rd Quarter.' in pbp_df.play.tolist():
        q3_ind=pbp_df.play.tolist().index('End of the 3rd Quarter.')
    elif 'Start of the 4th Quarter' in pbp_df.play.tolist():
        q3_ind=pbp_df.play.tolist().index('Start of the 4th Quarter')
    elif 'Start of the 4th Quarter.' in pbp_df.play.tolist():
        q3_ind=pbp_df.play.tolist().index('Start of the 4th Quarter.')
    else:
        q3_ind=-1
        
    if 'End of the 4th Quarter' in pbp_df.play.tolist():
        q4_ind=pbp_df.play.tolist().index('End of the 4th Quarter')
    elif 'End of the 4th Quarter.' in pbp_df.play.tolist():
        q4_ind=pbp_df.play.tolist().index('End of the 4th Quarter.')
    elif 'End of the Game' in pbp_df.play.tolist():
        q4_ind=pbp_df.play.tolist().index('End of the Game')
    elif 'Start of the 2nd Quarter.' in pbp_df.play.tolist():
        q4_ind=pbp_df.play.tolist().index('Start of the 2nd Quarter.')
    else:
        q4_ind=-1
        
    if 'End of Game' in pbp_df.play.tolist():
        ge_ind=pbp_df.play.tolist().index('End of Game')
    elif 'End of the Game' in pbp_df.play.tolist():
        ge_ind=pbp_df.play.tolist().index('End of the Game')
    else:
        ge_ind=-1
        
    try:
        ot1_ind=pbp_df.play.tolist().index('End of the 1st  Overtime')
    except:
        try:
            ot1_ind=pbp_df.play.tolist().index('End of the 1st  Overtime.')
        except:
            ot1_ind=-1
    try:
        ot2_ind=pbp_df.play.tolist().index('End of the 2nd  Overtime')
    except:
        try:
            ot2_ind=pbp_df.play.tolist().index('End of the 2nd  Overtime.')
        except:
            ot2_ind=-1
    try:
        ot3_ind=pbp_df.play.tolist().index('End of the 3rd  Overtime')
    except:
        try:
            ot3_ind=pbp_df.play.tolist().index('End of the 3rd  Overtime.')
        except:
            ot3_ind=-1
    try:
        ot4_ind=pbp_df.play.tolist().index('End of the 4th  Overtime')
    except:
        try:
            ot4_ind=pbp_df.play.tolist().index('End of the 4th  Overtime.')
        except:
            ot4_ind=-1
    try:
        ot5_ind=pbp_df.play.tolist().index('End of the 5th  Overtime')
    except:
        try:
            ot5_ind=pbp_df.play.tolist().index('End of the 5th  Overtime.')
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
            
'''
def get_Player(x):
    play=x.play.lower()
    keywords=['shooting','misses','offensive',
          'defensive','makes','loose',"'s",'enters','20 Sec',
          'Full','bad','delay', 'non-unsportsmanlike','steps',
          'out','flagrant','technical','kicked','palming','no turnover'
          'illegal','out of','3 second','post','possession','hanging on rim',
          'double','back court','punched','disc dribble','discontinued',
          'taunting','possession','double','technical','post',
          'personal','turnover','lost ball']
    #certain statements are not player-specific
    if play[:8] == 'jumpball'\
        or "coach's challenge" in play\
        or 'sec. time' in play\
        or 'full time' in play\
        or ' vs. ' in play:
            return None
    else:
        for kw in keywords:
            try:
                if kw=="'s":
                    player=play[play.index('blocks')+7:play.index(kw)].strip()
                else:
                    player=play[:play.index(kw)].strip()
                if any(x.isupper() for x in player):
                    return player
            except:
                continue
'''   
            
def get_Player(x):
    play=x.play
    keywords=['shooting','misses','offensive',
          'defensive','makes','loose',"'s",'enters','20 Sec',
          'full ','bad ','delay', 'non-unsportsmanlike','steps',
          'out ','flagrant','technical','kicked','palming','no turnover','traveling',
          'illegal','out of','3 second','possession','hanging on rim',
          'inbound','clear path','double','back court','punched','disc dribble',
          'discontinu','taunting','possession','double','technical','post ','jump ball',
          'personal','turnover','lost ball']
    #certain statements are not player-specific
    if play.lower()[:8] == 'jumpball'\
        or 'quarter' in play.lower()\
        or 'end game' in play.lower()\
        or ' vs. ' in play:
            return None
            
    else:
        start_index=0
        kw_index=100
        for kw in keywords:
            if kw in play.lower():
                #blocked shot syntax treated differently
                if kw=="'s":
                    if 'blocks' in play.lower():
                        start_index=play.index('blocks')+7
                    elif 'rejects' in play.lower():
                        start_index=play.index('rejects')+7
                    elif 'swats' in play.lower():
                        start_index=play.index('swats')+7
                else:
                    start_index=0
                    
                if play.lower().index(kw) < kw_index:
                    kw_index=play.lower().index(kw)
                
        player=play[start_index:kw_index].strip()
        if any(x.isupper() for x in player):
            return player
        else:
            return None
            
            
        
#Classify play type based on text               
def get_PlayType(x):
    play=x.play.lower()
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
    if 'goaltending' in play:
        return 'Goaltending'
    if 'delay of game' in play or 'lane violaton' in play or 'technical' in play:
        return 'violation'
    if ('turnover' in play and 'no turnover' not in play)\
    or 'bad pass' in play or 'steps out' in play\
    or 'travel' in play or 'lost ball' in play \
    or 'backcourt' in play or 'shot clock violation' in play \
    or 'discontinue dribble' in play or 'double dribble' in play \
    or 'out of bounds' in play or 'steps' in play \
    or 'charge' in play or ('foul' in play and 'offensive' in play):
        return 'Turnover'
    if '3 second' in play or 'illegal defense' in play:
        return '3 second/illegal defense'
    if 'foul' in play and 'offensive' not in play:
        return 'Foul'
    if 'kicked ball' in play:
        return 'Kicked Ball'
    if 'start of' in play and 'quarter' in play:
        return 'Quarter Start'
    if 'end of' in play and 'quarter' in play:
        return 'Quarter End'
    if 'end of' in play and 'game' in play:
        return 'Game End'
    if 'start of' in play and 'overtime' in play:
        return 'OT Start'
    if 'end of' in play and 'overtime' in play:
        return 'OT End'
    
    
#Get player responsible for event        
def get_Assistor(x):
    if 'assists' in x.play:
        return x.play[x.play.index('(')+1:x.play.index('assists')].strip()
         
         
def get_StolenBy(x):
    try:
        if 'steals' in x.play:
            return x.play[x.play.index('(')+1:x.play.index('steals')].strip()
    except:
        return None
            
def get_BlockedBy(x):
    if 'blocks' in x.play:
        return x.play[:x.play.index('blocks')].strip()
    elif 'rejects' in x.play:
        return x.play[:x.play.index('rejects')].strip()
    elif 'swats' in x.play:
        return x.play[:x.play.index('swats')].strip()
        
def get_Points(x,df):
    if 'makes' in x.play:
        if 'free throw' in x.play:
            return 1
        elif 'three point' in x.play:
            return 3
        else:
            return 2
    else:
        return 0
        

def get_SubbedIn(x):
    if 'enters' in x.play:
        return x.play[:x.play.index('enters')].strip()


def get_SubbedOut(x):
    try:
        if x.Play=='JR Smith enters the game for':
            return 'Kyle Korver'
        if 'enters' in x.play:
            return x.play[x.play.index('game for ')+9:]
    except:
        return None




def append_pbp(game_id,engine):
    
    url='http://www.espn.com/nba/playbyplay?gameId='+str(game_id)
    
    page = urlopen(url)
    
    content=page.read()
    soup=BeautifulSoup(content,'lxml')  
    
    away_team,home_team=[team.string for team in soup.findAll('span', attrs={'class':'abbrev'})]
    
    pbp_df=pd.DataFrame({'time':np.array([res.string for res in soup.find_all('td',attrs={'class','time-stamp'})]),
                         'play':np.array([res.string for res in soup.find_all('td',attrs={'class','game-details'})]),
                         'score':np.array([res.string for res in soup.find_all('td',attrs={'class','combined-score'})])})
    
    pbp_df['game_id']=[game_id]*len(pbp_df)
    #PlayByPlay['Date']=[x.Date]*len(PlayByPlay)
    
    if len(pbp_df) > 0:
        try:
            pbp_df['team']=[get_Team(res['src'],home_team,away_team) for res in soup.find_all('img',attrs={'class','team-logo'})][len(pbp_df)*-1:] 
        except:
            pbp_df['team']=None
            
        pbp_df['home_score']=pbp_df.apply(lambda x: get_HomeScore(x,home_team,away_team),axis=1)
        pbp_df['away_score']=pbp_df.apply(lambda x: get_AwayScore(x,home_team,away_team),axis=1)
        pbp_df['quarter']=pbp_df.apply(lambda x: get_Quarter(x,pbp_df),axis=1)
        pbp_df['player']=pbp_df.apply(lambda x: get_Player(x),axis=1)
        pbp_df['play_type']=pbp_df.apply(lambda x: get_PlayType(x),axis=1)   
        pbp_df['points']=pbp_df.apply(lambda x: get_Points(x,pbp_df),axis=1)    
        pbp_df['assistor']=pbp_df.apply(lambda x: get_Assistor(x),axis=1)
        pbp_df['stolen_by']=pbp_df.apply(lambda x: get_StolenBy(x),axis=1)   
        pbp_df['blocked_by']=pbp_df.apply(lambda x: get_BlockedBy(x),axis=1) 
        pbp_df['subbed_in']=pbp_df.apply(lambda x: get_SubbedIn(x),axis=1)   
        pbp_df['subbed_out']=pbp_df.apply(lambda x: get_SubbedOut(x),axis=1) 
    else:
        pbp_df['team']=[]
        pbp_df['team_score']=[]
        pbp_df['opp_team_score']=[]
        pbp_df['quarter']=[]
        pbp_df['player']=[]
        pbp_df['play_type']=[]
        pbp_df['points']=[]
        pbp_df['assistor']=[]
        pbp_df['stolen_by']=[]
        pbp_df['blocked_by']=[]
        pbp_df['subbed_in']=[]
        pbp_df['subbed_out']=[]
    
    column_order=['game_id','play','score','time','team','home_score','away_score',
               'quarter','player','play_type','points','assistor','stolen_by',
               'blocked_by','subbed_in','subbed_out']
    
    pbp_df[column_order].to_sql('play_by_play',
                                con=engine,
                                schema='nba',
                                index=False,
                                if_exists='append',
                                dtype={'game_id': sa.types.INTEGER(),
                                       'play': sa.types.VARCHAR(length=255),
                                       'score': sa.types.VARCHAR(length=255),
                                       'time': sa.types.VARCHAR(length=255),
                                       'team': sa.types.VARCHAR(length=255),
                                       'home_score': sa.types.INTEGER(),
                                       
                                       'away_score': sa.types.INTEGER(),
                                       'quarter': sa.types.VARCHAR(length=255),
                                       'player': sa.types.VARCHAR(length=255),
                                       'play_type': sa.types.VARCHAR(length=255),
                                       'points': sa.types.INTEGER(),
                                       
                                       'assistor': sa.types.VARCHAR(length=255),
                                       'stolen_by': sa.types.VARCHAR(length=255),
                                       'blocked_by': sa.types.VARCHAR(length=255),
                                       'subbed_in': sa.types.VARCHAR(length=255),
                                       'subbed_out': sa.types.VARCHAR(length=255)})
                 

#Get credentials stored in sql.yaml file (saved in root directory)
def get_engine():
    if os.path.isfile('/Users/dh08loma/Documents/Projects/Bracket Voodoo/sql.yaml'):
        with open("/Users/dh08loma/Documents/Projects/Bracket Voodoo/sql.yaml", 'r') as stream:
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
        nba.play_by_play p on gs.game_id=p.game_id
    left join
        nba.bad_gameids b on gs.game_id=b.game_id and b.table='play_by_play'
    where
        p.game_id is Null
        and b.game_id is Null
        and gs.status='Final'
    order by
        gs.season
    '''
    
    game_ids=pd.read_sql(game_id_query,engine)
    
    return game_ids.game_id.tolist()

 
def update_play_by_play(engine,game_id_list):
    cnt=0
    print('Total Games: ',len(game_id_list))
    for game_id in game_id_list:
    
        try:
            append_pbp(game_id,engine)
            cnt+=1
            if np.mod(cnt,100)==0:
                print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
            
        except:
            bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['play_by_play']})
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
    update_play_by_play(engine,game_ids)
    
    
    
if __name__ == "__main__":
    main()       
        

