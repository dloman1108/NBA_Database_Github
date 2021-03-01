with subs_cte as (
    select	
        pbp.game_id
        ,pbp.time
        ,pbp.quarter
        ,pbp.play
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
        ,pbp.team_abbr
        ,pbp.home_score
        ,pbp.away_score
        ,pbp.home_score - pbp.away_score home_score_delta
        ,pbp.away_score - pbp.home_score away_score_delta
        --,pbp."SubbedIn"
        --,pbp."SubbedOut"
        ,coalesce(pri.player_name,'Null Player') subbed_in_player_name
        ,coalesce(pro.player_name,'Null Player') subbed_out_player_name
        ,coalesce(pri.player_id,0) subbed_in_player_id
        ,coalesce(pro.player_id,0) subbed_out_player_id
    from
        nba.play_by_play pbp
    join
        nba.game_summaries gs on pbp.game_id=gs.game_id 
        and gs.game_id = {}
    left join
        nba.player_reference pri on pbp.subbed_in=pri.player_name
    left join
        nba.player_reference pro on pbp.subbed_out=pro.player_name
    where
        play_type='Substitution'
)

select
    *
    ,coalesce((lead(game_time) over (partition by game_id order by game_time,subbed_in_player_id)) - game_time,
        case 
            when quarter = '4' then 48 - game_time
            when quarter = 'OT1' then 53 - game_time 
            when quarter = 'OT2' then 58 - game_time
            when quarter = 'OT3' then 63 - game_time
            when quarter = 'OT4' then 68 - game_time
            else Null
        end) time_delta
from
    subs_cte
