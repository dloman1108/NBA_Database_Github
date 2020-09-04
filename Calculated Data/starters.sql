with starters as (		
    select
        pb.game_id
        ,gs.season
        ,pr.player_name player
        ,pb.player_id
        ,pb.team_abbr
        ,case when pb.team_abbr=gs.home_team_abbr then 'Home' else 'Away' end home_away
        ,concat('Player',' ',cast(row_number() over (partition by pb.game_id,pb.team_abbr order by pb.player) as varchar)) rn
    from 
        nba.player_boxscores pb
    join
        nba.game_summaries gs on pb.game_id=gs.game_id 
        and gs.game_id = {} 
    join
        nba.player_reference pr on pb.player_id=pr.player_id
    where
        starter_flg=true
)

select	
    game_id
    ,season
    ,max(case when home_away='Home' then team_abbr else 'aaa' end) home_team_abbr
    ,max(case when home_away='Away' then team_abbr else 'aaa' end) away_team_abbr
    ,max(case when home_away='Home' and rn='Player 1' then player else 'aaa' end) home_player1
    ,max(case when home_away='Home' and rn='Player 1' then player_id else '0' end) home_player1_id
    ,max(case when home_away='Home' and rn='Player 2' then player else 'aaa' end) home_player2
    ,max(case when home_away='Home' and rn='Player 2' then player_id else '0' end) home_player2_id
    ,max(case when home_away='Home' and rn='Player 3' then player else 'aaa' end) home_player3
    ,max(case when home_away='Home' and rn='Player 3' then player_id else '0' end) home_player3_id
    ,max(case when home_away='Home' and rn='Player 4' then player else 'aaa' end) home_player4
    ,max(case when home_away='Home' and rn='Player 4' then player_id else '0' end) home_player4_id
    ,max(case when home_away='Home' and rn='Player 5' then player else 'aaa' end) home_player5
    ,max(case when home_away='Home' and rn='Player 5' then player_id else '0' end) home_player5_id
    
    ,max(case when home_away='Away' and rn='Player 1' then player else 'aaa' end) away_player1
    ,max(case when home_away='Away' and rn='Player 1' then player_id else '0' end) away_player1_id
    ,max(case when home_away='Away' and rn='Player 2' then player else 'aaa' end) away_player2
    ,max(case when home_away='Away' and rn='Player 2' then player_id else '0' end) away_player2_id
    ,max(case when home_away='Away' and rn='Player 3' then player else 'aaa' end) away_player3
    ,max(case when home_away='Away' and rn='Player 3' then player_id else '0' end) away_player3_id
    ,max(case when home_away='Away' and rn='Player 4' then player else 'aaa' end) away_player4
    ,max(case when home_away='Away' and rn='Player 4' then player_id else '0' end) away_player4_id
    ,max(case when home_away='Away' and rn='Player 5' then player else 'aaa' end) away_player5
    ,max(case when home_away='Away' and rn='Player 5' then player_id else '0' end) away_player5_id
    ,0 game_time
    ,0 home_score
    ,0 away_score
    ,'Beginning of game' play
    ,'12:00' as time
    ,1 quarter
from
    starters
group by
    game_id
    ,season