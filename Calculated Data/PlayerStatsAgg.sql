select 
    pb.player
    ,pb.player_id
    ,tsa.team
    ,pb.team_abbr
    ,tsa.team_id
    ,gs.season
    ,gs.game_type
    ,count(*) gp
    ,sum(pb.mp) mp
    ,avg(pb.pts) ppg
    ,avg(pb.fgm) fgm
    ,avg(pb.fga) fga
    ,sum(pb.fgm)*1.0/sum(pb.fga) fg_pct
    ,avg(pb.fg3m) fg3m
    ,avg(pb.fg3a) fg3a
    ,case when sum(pb.fg3a) > 0 then sum(pb.fg3m)*1.0/sum(pb.fg3a) else Null end fg3_pct
    ,avg(pb.ftm) ftm
    ,avg(pb.fta) fta
    ,sum(pb.ftm)*1.0/sum(pb.fta) ft_pct
    ,avg(pb.oreb) oreb
    ,avg(pb.dreb) dreb
    ,avg(pb.reb) reb
    ,avg(pb.ast) ast
    ,avg(pb.stl) stl
    ,avg(pb.blk) blk
    ,avg(pb.tov) tov
    ,avg(pb.pf) pf
    ,sum(pb.plus_minus) raw_plus_minus
    ,sum(pb.pts)*36.0/sum(pb.mp) pts_36
    ,sum(pb.fgm)*36.0/sum(pb.mp) fgm_36
    ,sum(pb.fga)*36.0/sum(pb.mp) fga_36
    ,sum(pb.fg3m)*36.0/sum(pb.mp) fg3m_36
    ,sum(pb.fg3a)*36.0/sum(pb.mp) fg3a_36
    ,sum(pb.ftm)*36.0/sum(pb.mp) ftm_36
    ,sum(pb.fta)*36.0/sum(pb.mp) fta_36
    ,sum(pb.oreb)*36.0/sum(pb.mp) oreb_36
    ,sum(pb.dreb)*36.0/sum(pb.mp) dreb_36
    ,sum(pb.reb)*36.0/sum(pb.mp) reb_36
    ,sum(pb.ast)*36.0/sum(pb.mp) ast_36
    ,sum(pb.stl)*36.0/sum(pb.mp) stl_36
    ,sum(pb.blk)*36.0/sum(pb.mp) blk_36
    ,sum(pb.tov)*36.0/sum(pb.mp) tov_36
    ,sum(pb.pf)*36.0/sum(pb.mp) pf_36
    ,(sum(pb.fgm)+sum(pb.fg3m)*.5)/sum(pb.fga) efg_pct
    ,sum(pb.pts)/(2*(sum(pb.fga)+0.44*sum(pb.fta))) ts_pct
    ,sum(pb.fg3a)*1.0/sum(pb.fga) fg3_rate
    ,sum(pb.fta)*1.0/sum(pb.fga) ft_rate
    ,(sum(pb.oreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.oreb*tsa.gp+tsa.dreb_opp*tsa.gp)) oreb_pct
    ,(sum(pb.dreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.dreb*tsa.gp+tsa.oreb_opp*tsa.gp)) dreb_pct
    ,(sum(pb.reb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.reb*tsa.gp+tsa.reb_opp*tsa.gp)) reb_pct
    ,sum(pb.ast)*1.0/(((sum(pb.mp)/(tsa.mp/5))*tsa.fgm*tsa.gp)-sum(pb.fgm)) ast_pct
    ,sum(pb.stl)*1.0/((sum(pb.mp)/(tsa.mp/5))*tsa.poss) stl_pct
    ,sum(pb.blk)*1.0/((sum(pb.mp)/(tsa.mp/5))*(tsa.fga_opp*tsa.gp-tsa.fg3a_opp*tsa.gp)) blk_pct
    ,sum(pb.tov)*1.0/(sum(pb.fga)+0.44*sum(pb.fta)+sum(pb.tov)) tov_pct
    ,(sum(pb.fga)+0.44*sum(pb.fta)+sum(pb.tov))/((sum(pb.mp)/(tsa.mp/5))*(tsa.fga*tsa.gp+0.44*tsa.fta*tsa.gp+tsa.tov*tsa.gp)) usg_pct   
    ,now() last_update_dts
from 
    nba.player_boxscores pb 
join
    nba.game_summaries gs on pb.game_id=gs.game_id
left join
    nba.team_stats_agg tsa on pb.team_abbr=tsa.team_abbr and gs.game_type=tsa.game_type and gs.season=tsa.season
where 1=1
    and pb.dnp_reason is null
    and pb.mp > 0
group by
    pb.player
    ,pb.player_id
    ,tsa.team
    ,pb.team_abbr
    ,tsa.team_id
    ,gs.season
    ,gs.game_type
    ,tsa.mp
    ,tsa.gp
    ,tsa.dreb
    ,tsa.oreb_opp
    ,tsa.oreb
    ,tsa.dreb_opp
    ,tsa.reb
    ,tsa.reb_opp
    ,tsa.fgm
    ,tsa.poss
    ,tsa.fga_opp
    ,tsa.fg3a_opp
    ,tsa.fga
    ,tsa.fta
    ,tsa.tov
having
    sum(pb.fga) > 0 and sum(pb.fta) > 0 and sum(pb.tov) > 0