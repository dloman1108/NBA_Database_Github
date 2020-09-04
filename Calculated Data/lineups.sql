with pbp_plus_game_time as (
    select 
        pbp.game_id
        ,pbp.team_abbr
        ,pbp.play_type
        ,pbp.play
        ,pbp.points
        ,pbp.assistor
        ,pbp.stolen_by
        ,case
            when pbp.quarter like 'OT%'
                then 48 + (5 * (cast(substring(quarter,3,1) as int) - 1)) + (5 - time_minutes)
            else 
                12 * (cast(quarter as int) - 1) + (12 - time_minutes)
            end game_time
    from
        nba_sandbox.play_by_play pbp
    join   
        nba.game_summaries gs 
        on pbp.game_id=gs.game_id
        and gs.season >= 2018
)

select 
    ls.game_id
    ,ls.home_team_abbr
    ,ls.away_team_abbr
    ,ls.home_player1
    ,ls.home_player1_id
    ,ls.home_player2
    ,ls.home_player2_id
    ,ls.home_player3
    ,ls.home_player3_id
    ,ls.home_player4
    ,ls.home_player4_id
    ,ls.home_player5
    ,ls.home_player5_id
    ,ls.away_player1
    ,ls.away_player1_id
    ,ls.away_player2
    ,ls.away_player2_id
    ,ls.away_player3
    ,ls.away_player3_id
    ,ls.away_player4
    ,ls.away_player4_id
    ,ls.away_player5
    ,ls.away_player5_id
    ,ls.home_score
    ,ls.away_score
    --,ls.play
    ,ls.time
    ,ls.quarter
    ,ls.game_time
    ,ls.time_delta
    ,coalesce(ls.home_score_delta,0) home_score_delta
    ,coalesce(ls.away_score_delta,0) away_score_delta
    ,coalesce(ls.time_remaining,48) time_remaining

    --Estimate possessions: (FGA + . 44*FTA – ORB + TOV)/2
    --home team poss
    ,(sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type in ('Made Field Goal','Missed Field Goal') then 1 else 0 end) --home_team_fga
    + sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type in ('Made Free Throw','Missed Free Throw') then 1 else 0 end)*.44 --home_team_fta
    - sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type = 'Offensive Rebound' then 1 else 0 end) --home_team_oreb
    + sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type = 'Turnover' then 1 else 0 end) --home_team_tov
    ) home_team_possessions

    --away team poss
    ,(sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type in ('Made Field Goal','Missed Field Goal') then 1 else 0 end) --away_team_fga
    + sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type in ('Made Free Throw','Missed Free Throw')  then 1 else 0 end)*.44 --away_team_fta
    - sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type = 'Offensive Rebound' then 1 else 0 end) --away_team_oreb
    + sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type = 'Turnover' then 1 else 0 end) --away_team_tov
    ) away_team_possessions

    --home team pts scored
    , sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        then points else 0 end) home_team_pts
    --away team pts scored
    , sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        then points else 0 end) away_team_pts

    --Get other metrics: reb, fg, ast, stl, tov, fls 
    /* Rebounds */
    --home team reb
    , sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type in ('Offensive Rebound','Defensive Rebound') then 1 else 0 end) home_team_reb
    --away team reb
    , sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type in ('Offensive Rebound','Defensive Rebound') then 1 else 0 end) away_team_reb
    --total reb
    , sum(case 
        when 1=1
        and pbp.play_type in ('Offensive Rebound','Defensive Rebound') then 1 else 0 end) total_reb
    
    /* Shots and Assists */
    --home team fgm
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type = 'Made Field Goal' then 1 else 0 end) home_team_fgm
    --home team fga
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type in ('Made Field Goal','Missed Field Goal') then 1 else 0 end) home_team_fga
    --home team 3PM
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type = 'Made Field Goal'
        and pbp.play like '%3 point%' then 1 else 0 end) home_team_3pm
    --home team 3PA
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type in ('Made Field Goal','Missed Field Goal')
        and pbp.play like '%3 point%' then 1 else 0 end) home_team_3pa
    --home team ast
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.assistor is not Null then 1 else 0 end) home_team_ast
  
    --away team fgm
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type = 'Made Field Goal' then 1 else 0 end) away_team_fgm
    --away team fga
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type in ('Made Field Goal','Missed Field Goal') then 1 else 0 end) away_team_fga
    --away team 3PM
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type in ('Made Field Goal')
        and pbp.play like '%3 point%' then 1 else 0 end) away_team_3pm
    --away team 3PA
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type in ('Made Field Goal','Missed Field Goal')
        and pbp.play like '%3 point%' then 1 else 0 end) away_team_3pa
    --away team ast
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.assistor is not Null then 1 else 0 end) away_team_ast
  
    /* Other - steals, turnovers and fouls */
    --home team steals
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.stolen_by is not Null then 1 else 0 end) home_team_stl
    --home team turnovers
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type = 'Turnover' then 1 else 0 end) home_team_tov
    --home team fouls
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.play_type = 'Foul' then 1 else 0 end) home_team_fls

    --away team steals
    ,sum(case 
        when pbp.team_abbr = ls.home_team_abbr 
        and pbp.stolen_by is not Null then 1 else 0 end) away_team_stl
    --away team turnovers
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type = 'Turnover' then 1 else 0 end) away_team_tov
    --away team fouls
    ,sum(case 
        when pbp.team_abbr = ls.away_team_abbr 
        and pbp.play_type = 'Foul' then 1 else 0 end) away_team_fls
from 
    nba_sandbox.lineups_stage ls
join
    pbp_plus_game_time pbp
    on ls.game_id=pbp.game_id
    and pbp.game_time > ls.game_time + .001 and pbp.game_time <= ls.game_time + ls.time_delta + .001
where 1=1
    and ls.time_delta > 0
    and ls.game_id in {}
group by
    ls.game_id
    ,ls.home_team_abbr
    ,ls.away_team_abbr
    ,ls.home_player1
    ,ls.home_player1_id
    ,ls.home_player2
    ,ls.home_player2_id
    ,ls.home_player3
    ,ls.home_player3_id
    ,ls.home_player4
    ,ls.home_player4_id
    ,ls.home_player5
    ,ls.home_player5_id
    ,ls.away_player1
    ,ls.away_player1_id
    ,ls.away_player2
    ,ls.away_player2_id
    ,ls.away_player3
    ,ls.away_player3_id
    ,ls.away_player4
    ,ls.away_player4_id
    ,ls.away_player5
    ,ls.away_player5_id
    ,ls.home_score
    ,ls.away_score
    ,ls.play
    ,ls.time
    ,ls.quarter
    ,ls.game_time
    ,ls.time_delta  
    ,ls.home_score_delta
    ,ls.away_score_delta
    ,ls.time_remaining