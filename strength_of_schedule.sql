-- Strength of Schedule, using SOS = (2(OR)+(OOR))/3
with season_games as (
	select *
	from data.games g
	where g.season = 16 and g.day < 45
),
record as (
	select 	t.team_id, 
			t.nickname,
			--g.*
			count(g.game_id) filter (where (t.team_id = g.home_team and g.home_score>g.away_score) or (t.team_id = g.away_team and g.home_score<g.away_score)) as wins,
			count(g.game_id) filter (where (t.team_id = g.home_team and g.home_score<g.away_score) or (t.team_id = g.away_team and g.home_score>g.away_score)) as loses
	from season_games g
	join data.teams t on (t.team_id = g.home_team or t.team_id = g.away_team) and t.valid_until is null
	group by t.team_id, t.nickname
),
opp_record as (
	select 	r.team_id,
			r.nickname,
			sum(opp.wins) as wins,
			sum(opp.loses) as loses
	from season_games g
	join record r on (r.team_id = g.home_team or r.team_id = g.away_team)
	join record opp on (opp.team_id = g.home_team or opp.team_id = g.away_team) and r.team_id != opp.team_id
	group by 	r.team_id,
				r.nickname
),
opp_opp_record as (
	select 	r.team_id,
			r.nickname,
			sum(opp.wins) as wins,
			sum(opp.loses) as loses
	from season_games g
	join opp_record r on (r.team_id = g.home_team or r.team_id = g.away_team)
	join opp_record opp on (opp.team_id = g.home_team or opp.team_id = g.away_team) and r.team_id != opp.team_id
	group by 	r.team_id,
				r.nickname
)
select 	r.team_id,
		r.nickname,
		r.wins,
		r.loses,
		o_r.wins as opp_wins,
		o_r.loses as opp_loses,
		oor.wins as opp_opp_wins,
		oor.loses as opp_opp_loses,
		trunc((2*(o_r.wins::decimal/(o_r.wins+o_r.loses))+(oor.wins::decimal/(oor.wins+oor.loses)))/3, 4) as sos
from record r
join opp_record o_r on r.team_id = o_r.team_id
join opp_opp_record oor on r.team_id = oor.team_id
order by sos desc

