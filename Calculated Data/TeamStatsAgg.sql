with possessions as (
    select
        tb.team_abbr
        ,gs.season
        ,gs.game_type
        ,.5*((sum(tb.fga)+0.4*sum(tb.fta)-1.07*(sum(tb.oreb)*1.0/(sum(tb.oreb)+sum(tb.dreb_opp)))*(sum(tb.fga)-sum(tb.fgm))+sum(tb.tov))+(sum(tb.fga_opp)+0.4*sum(tb.fta_opp)-1.07*(sum(tb.oreb_opp)*1.0/(sum(tb.oreb_opp)+sum(tb.dreb))) * (sum(tb.fga_opp)-sum(tb.fgm_opp))+sum(tb.tov_opp))) poss
    from
        nba.team_boxscores tb
    join
        nba.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final'
    group by
        tb.team_abbr
        ,gs.season
        ,gs.game_type
)

select
    max(case 
        when tb.team_abbr=gs.home_team_abbr then gs.home_team
        when tb.team_abbr=gs.away_team_abbr then gs.away_team else Null end) team
    ,tb.team_abbr
    ,max(case 
        when tb.team_abbr=gs.home_team_abbr then gs.home_team_id 
        when tb.team_abbr=gs.away_team_abbr then gs.away_team_id else Null end) team_id
    ,gs.season
    ,gs.game_type
    ,count(*) gp
    ,sum(case when tb.pts > tb.pts_opp then 1 else 0 end) wins
    ,sum(case when tb.pts < tb.pts_opp then 1 else 0 end) losses
    ,avg(case when tb.pts > tb.pts_opp then 1.0 else 0 end) win_pct
    ,avg(tb.fgm*1.0) fgm
    ,avg(tb.fga*1.0) fga
    ,sum(tb.fgm)*1.0/sum(tb.fga) fg_pct
    ,avg(tb.fg3m*1.0) fg3m
    ,avg(tb.fg3a*1.0) fg3a
    ,sum(tb.fg3m)*1.0/sum(tb.fg3a) fg3_pct
    ,avg(tb.ftm*1.0) ftm
    ,avg(tb.fta*1.0) fta
    ,sum(tb.ftm)*1.0/sum(tb.fta) ft_pct
    ,avg(tb.pts) pts
    ,avg(tb.oreb)+avg(tb.dreb) reb
    ,avg(tb.oreb) oreb
    ,avg(tb.dreb) dreb
    ,avg(tb.ast) ast
    ,avg(tb.stl) stl
    ,avg(tb.blk) blk
    ,avg(tb.tov) tov
    ,avg(tb.pts_off_tov) pts_off_tov
    ,avg(tb.fst_brk_pts) fst_brk_pts
    ,avg(tb.pts_in_pnt) pts_in_pnt
    ,avg(tb.pf) pf
    ,avg(tb.tech_fl) tech_fl
    ,avg(tb.flag_fl) flag_fl
    ,avg(tb.fgm_opp*1.0) fgm_opp
    ,avg(tb.fga_opp*1.0) fga_opp
    ,sum(tb.fgm_opp)*1.0/sum(tb.fga_opp) fg_pct_opp
    ,avg(tb.fg3m_opp*1.0) fg3m_opp
    ,avg(tb.fg3a_opp*1.0) fg3a_opp
    ,sum(tb.fg3m_opp)*1.0/sum(tb.fg3a_opp) fg3_pct_opp
    ,avg(tb.ftm_opp*1.0) ftm_opp
    ,avg(tb.fta_opp*1.0) fta_opp
    ,sum(tb.ftm_opp)*1.0/sum(tb.fta_opp) ft_pct_opp
    ,avg(tb.pts_opp) pts_opp
    ,avg(tb.oreb_opp)+avg(tb.dreb_opp) reb_opp
    ,avg(tb.oreb_opp) oreb_opp
    ,avg(tb.dreb_opp) dreb_opp
    ,avg(tb.ast_opp) ast_opp
    ,avg(tb.stl_opp) stl_opp
    ,avg(tb.blk_opp) blk_opp
    ,avg(tb.tov_opp) tov_opp
    ,avg(tb.pts_off_tov_opp) pts_off_tov_opp
    ,avg(tb.fst_brk_pts_opp) fst_brk_pts_opp
    ,avg(tb.pts_in_pnt_opp) pts_in_pnt_opp
    ,avg(pf_opp) pf_opp
    ,avg(tech_fl_opp) tech_fl_opp
    ,avg(flag_fl_opp) flag_fl_opp
    --Get 4 factors + ratings
    ,sum(pb.mp) mp
    ,p.poss
    ,48*((p.poss*2)/(2*(sum(pb.mp)/5.0))) pace
    ,sum(tb.pts)/p.poss*100 off_rtg
    ,sum(tb.pts_opp)*100.0/p.poss def_rtg
    ,(sum(tb.pts)-sum(tb.pts_opp))/p.poss*100 net_rtg
    ,sum(tb.fg3a)*1.0/sum(tb.fga) fg3_rate
    ,sum(tb.fta)*1.0/sum(tb.fga) ft_rate
    ,(sum(tb.fgm)+.5*sum(tb.fg3m))/sum(tb.fga) efg_pct
    ,sum(tb.tov)/(sum(tb.fga)+.44*sum(tb.fta)+sum(tb.tov)) tov_pct
    ,sum(tb.oreb)*1.0/(sum(tb.oreb)+sum(tb.dreb_opp)) oreb_pct
    ,sum(tb.ftm)*1.0/sum(tb.fga) ff_ft_rate
    ,sum(tb.fg3a_opp)*1.0/sum(tb.fga_opp) fg3_rate_opp
    ,sum(tb.fta_opp)*1.0/sum(tb.fga_opp) ft_rate_opp
    ,(sum(tb.fgm_opp)+.5*sum(tb.fg3m_opp))/sum(tb.fga_opp) efg_pct_opp
    ,sum(tb.tov_opp)/(sum(tb.fga_opp)+.44*sum(tb.fta_opp)+sum(tb.tov_opp)) tov_pct_opp
    ,sum(tb.oreb_opp)*1.0/(sum(tb.oreb_opp)+sum(tb.dreb)) oreb_pct_opp
    ,sum(tb.ftm_opp)*1.0/sum(tb.fga_opp) ff_ft_rate_opp
    ,now() last_update_dts
from
    nba.team_boxscores tb
join
    nba.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final'
left join
    (select game_id,team_abbr,sum(cast(mp as float)) mp from nba.player_boxscores group by game_id,team_abbr) pb on tb.game_id=pb.game_id and tb.team_abbr=pb.team_abbr
join
    possessions p on tb.team_abbr=p.team_abbr and gs.season=p.season and gs.game_type=p.game_type
group by
    tb.team_abbr
    ,gs.season
    ,gs.game_type
    ,p.poss