-- Calculate the hype for each stadium on each day
-- Based on the understanding that a shame win is worth 0.27 hype, any other game is worth -0.03, and doesn't decay on days when no games are being played at that stadium.
with recursive  -- First one isn't recursive, but 'recursive' has to go at the top, and marks all the CTEs as allowing recursion
stadium_days as (
	select 	s.season, d.day, st.stadium_id, st.team_id, g.home_score, g.away_score, g.ended_in_shame,
			case when coalesce(g.game_id, '')='' then 0.0 when g.home_score>g.away_score and g.ended_in_shame then 0.27 else -0.03 end as hype_delta
	from data.stadiums st
	cross join generate_series(0, 120) as d(day) -- (mix,max) Extends past the maximum length of a season to catch every day
	cross join generate_series(18, 18) as s(season) -- (min,max) change to go back to calculate the hype as if hype existed earlier
	left join data.games g on g.season = s.season and g.day = d.day and st.team_id = g.home_team
	where st.valid_until is null
),
stadium_hype as (
	select 	sd.season, sd.day, sd.stadium_id, sd.team_id, sd.hype_delta, 
			0.0 as hype_before,
			case when sd.hype_delta>=0 then sd.hype_delta else 0.0 end as hype_after
	from stadium_days sd
	where sd.day=0
	union all
	select 	sd.season, sd.day, sd.stadium_id, sd.team_id, sd.hype_delta,
			sh.hype_after as hype_before,
			case when (sh.hype_after+sd.hype_delta)<0 then 0.0 else (sh.hype_after+sd.hype_delta) end as hype_after
	from stadium_hype sh
	join stadium_days sd on sd.season = sh.season and sd.day = sh.day+1 and sd.stadium_id = sh.stadium_id
)
-- Basically just join stadium_hype to game/game_event on season/day/home team, to find what the hype was (hype_before) for the game/event
-- Here is an example that shows the hype in effect for each game on s19d50
select 	g.season+1 as season, g.day+1 as day, 
		t_home.nickname, t_away.nickname,
		g.home_score, g.away_score, 
		sh.hype_before
from data.games g
join data.teams t_home on g.home_team = t_home.team_id and t_home.valid_until is null
join data.teams t_away on g.away_team = t_away.team_id and t_away.valid_until is null
join stadium_hype sh on sh.season = g.season and sh.day = g.day and sh.team_id = g.home_team
where g.season+1 = 19
and g.day+1 = 50