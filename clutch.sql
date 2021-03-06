-- TODO:
--		Fix GAME_OVER events, which currently have the same state as the beginning of the final inning, confusing the WE stuff
--		Filter out non-batting events from various parts of the calculations
--		Group run diffs of +11 together 
--		Filter our rare states (>4 bases, more than 3 outs, etc)
--
-- This is the list of all events we're looking to use, with extra info we need to calculate stuff later
with game_events_extra as (
	select  ge.id,
			ge.game_id,
			ge.season,
			ge.event_type,
			ge.event_text,
			ge.batter_id,
			ge.pitcher_id,
			ge.inning,
			(case when g.home_score>g.away_score then 1 else 0 end) as game_winning, -- did the home team end up winning?
			(case when ge.inning >= 8 then 8 else ge.inning end) as state_inning,
			ge.top_of_inning as state_top_of_inning,
			ceiling(ge.home_score - ge.away_score) as state_run_diff, -- +if the home team is ahead
			-- Not sure about using ceiling here. For decimal score diffs, if you're ahead it rounds up, but if you're behind it rounds down.
			ge.outs_before_play as state_outs,
			0+coalesce(bool_or(gebr.base_before_play = 1)::int, 0)*1
			+ coalesce(bool_or(gebr.base_before_play = 2)::int, 0)*2
			+ coalesce(bool_or(gebr.base_before_play = 3)::int, 0)*4
			+ coalesce(bool_or(gebr.base_before_play = 4)::int, 0)*8 -- 4th base nonsense
			as state_baserunners, -- Baserunner state encoded as a binary number (e.g. runners on 3rd and 2nd = 110 = 6)
			sum(ge.outs_on_play) over inning_window as state_outs_during_inning,
			(case when top_of_inning then ge.away_base_count else ge.home_base_count end) as state_base_count,
			lead(ge.id) over game_window as next_event_id
	from data.game_events ge
  	left join data.game_event_base_runners gebr on ge.id = gebr.game_event_id
	join data.games g on ge.game_id = g.game_id
  	where ge.season between 11 and 24 
  	and ge.day < 99
  	group by ge.id, g.game_id
  	window 
  		inning_window as (partition by ge.game_id, ge.inning, ge.top_of_inning order by ge.id range between unbounded preceding and unbounded following),
  		game_window as (partition by ge.game_id order by ge.id range between unbounded preceding and unbounded following)
),
-- Generate the average Win Expectency for any game state
WE_matrix as (
	select	gee.state_outs, 
			gee.state_baserunners, 
			gee.state_outs_during_inning, 
			gee.state_base_count, 
			gee.state_run_diff,
			gee.state_inning,
			gee.state_top_of_inning,
			avg(gee.game_winning) as WE, 
			count(*) as event_count
	from game_events_extra gee
	where 1=1
	group by 
			gee.state_outs, 
			gee.state_baserunners, 
			gee.state_outs_during_inning, 
			gee.state_base_count, 
			gee.state_run_diff, 
			gee.state_inning, 
			gee.state_top_of_inning
	order by 
			gee.state_outs_during_inning, 
			gee.state_base_count, 
			gee.state_inning, 
			gee.state_top_of_inning,
			gee.state_outs, 
			gee.state_baserunners, 
			gee.state_run_diff

),
-- Join each event to the following event, and use the matrix to look at the WE for each, and the change (WPA)
game_events_WE as (
	select 	gee_before.id,
			gee_before.game_id,
			gee_before.season,
			gee_before.event_type,
			gee_before.batter_id,
			gee_before.pitcher_id,
			gee_before.game_winning,

			we_before.WE as before_WE,
			we_after.WE as after_WE,
			coalesce(we_after.WE-we_before.WE, 0) as wpa,
			
			gee_before.state_outs_during_inning,
			gee_before.state_base_count,
			gee_before.state_run_diff as state_before_run_diff,
			gee_before.state_inning as state_before_inning,
			gee_before.state_top_of_inning as state_before_top_of_inning,
			gee_before.state_outs as state_before_outs,
			gee_before.state_baserunners as state_before_baserunners,
			
			gee_after.state_run_diff as state_after_run_diff,
			gee_after.state_inning as state_after_inning,
			gee_after.state_top_of_inning as state_after_top_of_inning,
			gee_after.state_outs as state_after_outs,
			gee_after.state_baserunners as state_after_baserunners

	from game_events_extra gee_before
	left join game_events_extra gee_after on gee_before.next_event_id = gee_after.id
	left join WE_matrix we_before 
			on gee_before.state_outs_during_inning = we_before.state_outs_during_inning
			and gee_before.state_base_count = we_before.state_base_count 
			and gee_before.state_inning = we_before.state_inning 
			and gee_before.state_top_of_inning = we_before.state_top_of_inning
			and gee_before.state_outs = we_before.state_outs 
			and gee_before.state_baserunners = we_before.state_baserunners 
			and gee_before.state_run_diff = we_before.state_run_diff
	left join WE_matrix we_after
			on gee_after.state_outs_during_inning = we_after.state_outs_during_inning
			and gee_after.state_base_count = we_after.state_base_count 
			and gee_after.state_inning = we_after.state_inning 
			and gee_after.state_top_of_inning = we_after.state_top_of_inning
			and gee_after.state_outs = we_after.state_outs 
			and gee_after.state_baserunners = we_after.state_baserunners 
			and gee_after.state_run_diff = we_after.state_run_diff
),
-- Determine the average change in WE for each state (swing).
leverage as (
	select	gew.state_outs_during_inning, 
			gew.state_base_count, 
			gew.state_before_inning,
			gew.state_before_top_of_inning,		
			gew.state_before_outs, 
			gew.state_before_baserunners, 
			gew.state_before_run_diff,
			avg(abs(gew.wpa)) as swing, 
			count(*) as event_count
	from game_events_WE gew
	where 1=1
	group by 
			gew.state_outs_during_inning, 
			gew.state_base_count, 
			gew.state_before_inning,
			gew.state_before_top_of_inning,		
			gew.state_before_outs, 
			gew.state_before_baserunners, 
			gew.state_before_run_diff
			
	order by 
			gew.state_outs_during_inning, 
			gew.state_base_count, 
			gew.state_before_inning,
			gew.state_before_top_of_inning,		
			gew.state_before_outs, 
			gew.state_before_baserunners, 
			gew.state_before_run_diff
),
-- Determine the average swing for any state. Used to normalise all swings to an average of 1, as Leverage Index (LI)
leverage_ref as (
	select avg(abs(gew.wpa)) as l_baseline
	from game_events_WE gew
),
-- Add the leverage information to each event, and the WPA/LI (WPA, normalised by the LI of the event, also called "Context Neutral Wins")
game_events_WELI as (
	select 	gew.*,
			l.swing,
			lr.l_baseline,
			l.swing/lr.l_baseline as LI,
			coalesce(gew.WPA/NULLIF(l.swing/lr.l_baseline, 0), 0) as wpa_li
	from game_events_we gew
	join leverage l
		on	gew.state_outs_during_inning = l.state_outs_during_inning 
		and	gew.state_base_count = l.state_base_count
		and gew.state_before_inning = l.state_before_inning
		and	gew.state_before_top_of_inning = l.state_before_top_of_inning
		and gew.state_before_outs = l.state_before_outs 
		and gew.state_before_baserunners = l.state_before_baserunners 
		and gew.state_before_run_diff = l.state_before_run_diff
	cross join leverage_ref lr 
),
-- Join the data to the players.
-- Since all WPA is calculated from the POV of the Home team, summing needs to be flipped depending on Batter or Pitching in the Top or Bottom of the inning.
player_WPA as (
	select 	p.player_id, 
			p.player_name,
			--gew.season,
			coalesce(sum(gew.wpa) filter (where ((not gew.state_before_top_of_inning) and gew.batter_id = p.player_id) or (gew.state_before_top_of_inning and gew.pitcher_id = p.player_id)), 0)
			-coalesce(sum(gew.wpa) filter (where (gew.state_before_top_of_inning and gew.batter_id = p.player_id) or (not gew.state_before_top_of_inning and gew.pitcher_id = p.player_id)), 0) as WPA,
			avg(gew.li) as pLI,
			coalesce(sum(gew.wpa_li)	filter (where (not gew.state_before_top_of_inning and gew.batter_id = p.player_id) or (gew.state_before_top_of_inning and gew.pitcher_id = p.player_id)), 0)
			-coalesce(sum(gew.wpa_li) filter (where (gew.state_before_top_of_inning and gew.batter_id = p.player_id) or (not gew.state_before_top_of_inning and gew.pitcher_id = p.player_id)), 0) as wpa_li,
			count(*) as event_count
	from data.players p
	join game_events_WELI gew on ((gew.batter_id = p.player_id or gew.pitcher_id = p.player_id)) and p.valid_until is null
	group by p.player_id, p.player_name--, gew.season
)

-- List requested data
select 	--p.*
		p.player_id, 
		p.player_name,
		--p.season+1 as season,
		trunc(coalesce(p.wpa, 0), 4) as wpa,
		trunc(p.pli, 4) as pli,
		trunc(p.wpa_li, 4) as "wpa/li",
		trunc(coalesce(p.wpa/nullif(p.pli, 0), 0)-p.wpa_li, 4) as clutch,
		-- ^ this nonsense is to prevent div/0 when the swing for some event is 0, causing the pli to be 0.
		p.event_count		
from player_WPA p
order by clutch desc


