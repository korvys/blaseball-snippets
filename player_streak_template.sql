-- Only the first two CTEs should need to be updated, and some options at the bottom for filtering.
--
-- valid_event_pool should be a list of events that has a .perceived_at, and some value to check 
-- 	These can be game_events, or you could map a .perceived_at onto a game, or season, etc.
--	Also need to output the player_id (rather than batter_id or whatever).
--
-- flagged_events will add a row number over the events (mostly to be easier to work with than datetime), and a .flag field that is true or false 
-- 	if the event is part of a streak
--
--
-- This will only work with streaks where an event is part of a streak or not.
-- Events that shouldn't be counted, but also shouldn't break the streak should be removed from valid_event_pool
-- 
-- Current example will find streaks of PAs with batter walking
--
with valid_event_pool as (
	select  ge.batter_id as player_id, ge.*
	from data.game_events ge
	--
	-- Non PA events shouldn't break the streak
	join taxa.event_types et on ge.event_type = et.event_type
	where et.plate_appearance = 1 
),
flagged_events as (
	select 	row_number() over (partition by vep.player_id order by vep.perceived_at) as event_seq,
			case when vep.event_type in ('WALK', 'CHARM_WALK', 'MIND_TRICK_WALK') then true else false end as flag,
			vep.*
	from valid_event_pool vep
),
new_streaks as (
	select 	case when (fe.flag and lag(not fe.flag, 1, false) over (partition by fe.player_id order by fe.event_seq)) then 1 else 0 end as new_streak,
			fe.*
	from flagged_events fe
),
streak_no as (
	select 	sum(ns.new_streak) over (partition by ns.player_id order by ns.event_seq) as streak_no,
			ns.*
	from new_streaks ns
	where ns.flag
),
streaks as (
	select 	min(sn.event_seq) as start, max(sn.event_seq) as end, sn.player_id, sn.streak_no, count(*) as length,
			max(count(*)) over (partition by sn.player_id) as max_streak
	from streak_no sn
	group by sn.player_id, sn.streak_no
)
select 	(case when g_start.season=-1 then 'Coffee Cup' else 'Season '||g_start.season+1 end)||', Day '||g_start.day+1 as flag_streak_start, 
		(case when g_end.season=-1 then 'Coffee Cup' else 'Season '||g_end.season+1 end)||', Day '||g_end.day+1 flag_streak_end,
		--s.player_id, 
		p.player_name,
		s.length as streak_length
from streaks s
join flagged_events fe_start on s.start = fe_start.event_seq and fe_start.player_id = s.player_id
join data.games g_start on g_start.game_id = fe_start.game_id
join flagged_events fe_end on s.end = fe_end.event_seq and fe_end.player_id = s.player_id
join data.games g_end on g_end.game_id = fe_end.game_id
join data.players p on p.player_id = s.player_id and p.valid_until is null
join data.team_roster tr on tr.player_id = p.player_id and tr.valid_until is null
where 1=1
--and s.player_id = '7a75d626-d4fd-474f-a862-473138d8c376' -- Single player
--and tr.team_id = 'eb67ae5e-c4bf-46ca-bbbc-425cd34182ff' -- Single team
--and tr.position_type_id in (0, 1) -- Active players only (0 and 1 are active, 2 and 3 are shadows)
--and s.count = s.max_streak -- Only find the top streak 
--and s.length >= 5 -- Limit results to streaks greater than <x>
order by s.length desc