select 
    pbp.game_id
    ,pbp.team_abbr
    ,gs.home_team_id team_id
    ,1 home_team_flg
    ,pbp.home_score - pbp.away_score score_delta
    ,case
        when pbp.quarter like 'OT%'
            then 48 + (5 * (cast(substring(quarter,3,1) as int) - 1)) + (5 - time_minutes)
        else 
            12 * (cast(quarter as int) - 1) + (12 - time_minutes)
        end game_time
    ,case
        when pbp.quarter like 'OT%'
            then time_minutes
        else 
            12 * (4 - cast(quarter as int)) + time_minutes
        end time_remaining
    ,gs.home_team_winner win_flg
from
    nba.play_by_play pbp
join   
    nba.game_summaries gs 
    on pbp.game_id=gs.game_id
    and gs.home_team_abbr=pbp.team_abbr
    and gs.game_type='Regular Season'
    and gs.home_team_id <= 30 --no ASG
    and pbp.quarter is not Null

union

select 
    pbp.game_id
    ,pbp.team_abbr
    ,gs.away_team_id team_id
    ,0 home_team_flg
    ,pbp.away_score - pbp.home_score score_delta
    ,case
        when pbp.quarter like 'OT%'
            then 48 + (5 * (cast(substring(quarter,3,1) as int) - 1)) + (5 - time_minutes)
        else 
            12 * (cast(quarter as int) - 1) + (12 - time_minutes)
        end game_time
    ,case
        when pbp.quarter like 'OT%'
            then time_minutes
        else 
            12 * (4 - cast(quarter as int)) + time_minutes
        end time_remaining
    ,gs.away_team_winner win_flg
from
    nba.play_by_play pbp
join   
    nba.game_summaries gs 
    on pbp.game_id=gs.game_id
    and gs.away_team_abbr=pbp.team_abbr
    and gs.game_type='Regular Season'
    and gs.home_team_id <= 30 --no ASG
    and pbp.quarter is not Null