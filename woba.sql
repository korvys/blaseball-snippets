-- This is the list of all events we're looking to use, with extra info we need to calculate stuff later
with game_events_extra as (
	select  ge.id,
			ge.game_id,
			ge.season,
			ge.inning,
			ge.event_type,
			ge.event_text,
			ge.top_of_inning,
			ge.batter_id,
			ge.outs_before_play,
			ge.outs_before_play + ge.outs_on_play as outs_after_play,
			0+coalesce(bool_or(gebr.base_before_play = 1)::int, 0)*1
			+ coalesce(bool_or(gebr.base_before_play = 2)::int, 0)*2
			+ coalesce(bool_or(gebr.base_before_play = 3)::int, 0)*4
			+ coalesce(bool_or(gebr.base_before_play = 4)::int, 0)*8 -- 4th base nonsense
			as bases_before_play, -- Baserunner state encoded as a binary number (e.g. runners on 3rd and 2nd = 110 = 6)
			0+coalesce(bool_or(gebr.base_after_play = 1)::int, 0)*1
			+ coalesce(bool_or(gebr.base_after_play = 2)::int, 0)*2
			+ coalesce(bool_or(gebr.base_after_play = 3)::int, 0)*4
			+ coalesce(bool_or(gebr.base_after_play = 4)::int, 0)*8 -- 4th base nonsense
			as bases_after_play,  -- bool_or crushes hand holding runners into one.
			coalesce(sum(gebr.runs_scored), 0) as runs_scored,
			sum(ge.outs_on_play) over inning_window as outs_during_inning,
			(case when top_of_inning then ge.away_base_count else ge.home_base_count end) as base_count,
			last_value(case when ge.top_of_inning then ge.away_score else ge.home_score end) over inning_window
				-case when ge.top_of_inning then ge.away_score else ge.home_score end as future_runs
--		  	(max(case when ge.top_of_inning then ge.away_score else ge.home_score end) over inning_window)-(case when ge.top_of_inning then ge.away_score else ge.home_score end) as future_runs   -- Max instead of last, if prefered.
	from data.game_events ge
  	left join data.game_event_base_runners gebr on ge.id = gebr.game_event_id
  	where ge.season = 17 
  	and ge.day < 99
  	group by ge.id
  	window inning_window as (partition by ge.game_id, ge.inning, ge.top_of_inning order by ge.id range between unbounded preceding and unbounded following)
),
-- Generate the average Run Expectency at for any game state
RE_matrix as (
	select	gee.outs_before_play as outs, 
			gee.bases_before_play as bases, 
			gee.outs_during_inning, 
			gee.base_count, 
			avg(gee.future_runs) as RE, 
			count(*) as event_count
	from game_events_extra gee
	where 1=1
	group by gee.outs_before_play, gee.bases_before_play, gee.outs_during_inning, gee.base_count
	order by gee.outs_during_inning, gee.base_count, outs, bases
),
-- Look at the before and after state of the event, determine the change in RE, and associate it with each event.
game_events_RE as ( -- 
	select  --gee.id, gee.event_type, gee.event_text, -- debuging
			gee.*,
			rem1.outs, rem1.bases, rem1.RE, -- debugging info
 			rem2.outs, rem2.bases, rem2.RE, -- debugging info
			(coalesce(rem2.RE, 0) - coalesce(rem1.RE, 0))+gee.runs_scored as RE_delta
	from game_events_extra gee
	left join RE_matrix rem1 
		on gee.outs_before_play = rem1.outs 
		and gee.bases_before_play = rem1.bases 
		and gee.outs_during_inning = rem1.outs_during_inning 
		and gee.base_count = rem1.base_count
	left join RE_matrix rem2 
		on gee.outs_after_play = rem2.outs 
		and gee.bases_after_play = rem2.bases 
		and gee.outs_during_inning = rem2.outs_during_inning
		and gee.base_count = rem2.base_count
),
-- Calculate the linear weights from average RE delta for each event type used in wOBA, and counts
lweights as (
	select	coalesce(avg(ger.re_delta) filter (where ger.event_type in ('WALK', 'CHARM_WALK', 'MIND_TRICK_WALK')), 0) as bb,
			count(ger.re_delta) filter (where ger.event_type in ('WALK', 'CHARM_WALK', 'MIND_TRICK_WALK')) as bb_count,
			coalesce(avg(ger.re_delta) filter (where ger.event_type in ('HIT_BY_PITCH')), 0) as hbp,
			count(ger.re_delta) filter (where ger.event_type in ('HIT_BY_PITCH')) as hbp_count,
			coalesce(avg(ger.re_delta) filter (where ger.event_type in ('SINGLE')), 0) as b1,
			count(ger.re_delta) filter (where ger.event_type in ('SINGLE')) as b1_count,
			coalesce(avg(ger.re_delta) filter (where ger.event_type in ('DOUBLE')), 0) as b2,
			count(ger.re_delta) filter (where ger.event_type in ('DOUBLE')) as b2_count,
			coalesce(avg(ger.re_delta) filter (where ger.event_type in ('TRIPLE')), 0) as b3,
			count(ger.re_delta) filter (where ger.event_type in ('TRIPLE')) as b3_count,
			coalesce(avg(ger.re_delta) filter (where ger.event_type in ('QUADRUPLE')), 0) as b4,
			count(ger.re_delta) filter (where ger.event_type in ('QUADRUPLE')) as b4_count,
			coalesce(avg(ger.re_delta) filter (where ger.event_type in ('HOME_RUN', 'HOME_RUN_5')), 0) as HR,
			count(ger.re_delta) filter (where ger.event_type in ('HOME_RUN', 'HOME_RUN_5')) as HR_count,
			coalesce(avg(ger.RE_delta) filter (where ger.event_type in (select event_type from taxa.event_types where plate_appearance = 1 and out = 1)), 0) as out,
			count(ger.RE_delta) filter (where ger.event_type in (select event_type from taxa.event_types where plate_appearance = 1 and out = 1)) as out_count,
			count(*) filter (where ger.event_type in (select event_type from taxa.event_types where plate_appearance = 1)) as PA_count
	from game_events_RE ger
	-- Only interested in events played under "normal" rules for calculating weights.
	where ger.outs_during_inning = 3 
	and ger.base_count = 4 
),
-- Calculate various values to be used as scaling factors.
woba_scale as (
	select  (((bb-out)*bb_count) + ((hbp-out)*hbp_count) + ((b1-out)*b1_count) + ((b2-out)*b2_count) + ((b3-out)*b3_count) + ((b4-out)*b4_count) + ((hr-out)*hr_count))/pa_count as league_woba,
			(bb_count + hbp_count + b1_count + b2_count + b3_count + b4_count + hr_count)::decimal/pa_count as league_obp,
			(bb_count + hbp_count + b1_count + b2_count + b3_count + b4_count + hr_count)::decimal/(((bb-out)*bb_count) + ((hbp-out)*hbp_count) + ((b1-out)*b1_count) + ((b2-out)*b2_count) + ((b3-out)*b3_count) + ((b4-out)*b4_count) + ((hr-out)*hr_count)) as scale
	from lweights
),
-- Scale the linear weights, to get final wOBA weights.
woba_weights as (
	select	trunc((l.bb-l.out)*(select scale from woba_scale), 3) as bb,
			trunc((l.hbp-l.out)*(select scale from woba_scale), 3) as hbp,
			trunc((l.b1-l.out)*(select scale from woba_scale), 3) as b1,
			trunc((l.b2-l.out)*(select scale from woba_scale), 3) as b2,
			trunc((l.b3-l.out)*(select scale from woba_scale), 3) as b3,
			trunc((l.b4-l.out)*(select scale from woba_scale), 3) as b4,
			trunc((l.hr-l.out)*(select scale from woba_scale), 3) as hr
	from lweights l
),
-- Calculate final wOBA value based on wOBA weights and the player's record.
player_woba as (
	select	p.player_name,
			trunc((count(*) filter (where ge.event_type in ('WALK', 'CHARM_WALK', 'MIND_TRICK_WALK')) * (select bb from woba_weights) +
			count(*) filter (where ge.event_type in ('HIT_BY_PITCH')) * (select hbp from woba_weights) +
			count(*) filter (where ge.event_type in ('SINGLE')) * (select b1 from woba_weights) +
			count(*) filter (where ge.event_type in ('DOUBLE')) * (select b2 from woba_weights) +
			count(*) filter (where ge.event_type in ('TRIPLE')) * (select b3 from woba_weights) +
			count(*) filter (where ge.event_type in ('QUADRUPLE')) * (select b4 from woba_weights) +
			count(*) filter (where ge.event_type in ('HOME_RUN', 'HOME_RUN_5')) * (select hr from woba_weights))::decimal/
			coalesce(nullif(count(*) filter (where ge.event_type in (select event_type from taxa.event_types where plate_appearance = 1)), 0), -1), 3) as wOBA,
			count(*) filter (where ge.event_type in (select event_type from taxa.event_types where plate_appearance = 1)) as pa
	from data.players p
	join data.game_events ge on ge.batter_id = p.player_id and p.valid_until is null
	where ge.season = 17
	group by p.player_name
	having count(*) filter (where ge.event_type in (select event_type from taxa.event_types where plate_appearance = 1)) >= 25
	order by wOBA desc
)
-- List requested data
select *
from player_woba
