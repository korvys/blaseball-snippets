with valid_event_pool as (
	select ge.*
	from data.game_events ge
	join taxa.event_types et on ge.event_type = et.event_type
	where et.plate_appearance = 1
),
flagged_events as (
	select 	row_number() over (partition by vep.batter_id order by vep.perceived_at) as event_seq,
			case when vep.event_type in ('WALK', 'CHARM_WALK', 'MIND_TRICK_WALK') then true else false end as flag,
			vep.*
	from valid_event_pool vep
),
new_streaks as (
	select 	case when (fe.flag and lag(not fe.flag, 1, false) over (partition by fe.batter_id order by fe.event_seq)) then 1 else 0 end as new_streak,
			fe.*
	from flagged_events fe
),
streak_no as (
	select 	sum(ns.new_streak) over (partition by ns.batter_id order by ns.event_seq) as streak_no,
			ns.*
	from new_streaks ns
	where ns.flag
),
streaks as (
	select 	min(sn.event_seq) as start, max(sn.event_seq) as end, sn.batter_id, sn.streak_no, count(*) as length,
			max(count(*)) over (partition by sn.batter_id) as max_streak
	from streak_no sn
	group by sn.batter_id, sn.streak_no
)
select 	(case when g_start.season=-1 then 'Coffee Cup' else 'Season '||g_start.season+1 end)||', Day '||g_start.day+1 as flag_streak_start, 
		(case when g_end.season=-1 then 'Coffee Cup' else 'Season '||g_end.season+1 end)||', Day '||g_end.day+1 flag_streak_end,
		--s.batter_id, 
		p.player_name,
		s.length as streak_length
from streaks s
join flagged_events fe_start on s.start = fe_start.event_seq and fe_start.batter_id = s.batter_id
join data.games g_start on g_start.game_id = fe_start.game_id
join flagged_events fe_end on s.end = fe_end.event_seq and fe_end.batter_id = s.batter_id
join data.games g_end on g_end.game_id = fe_end.game_id
join data.players p on p.player_id = s.batter_id and p.valid_until is null
where 1=1
--and s.batter_id = '7a75d626-d4fd-474f-a862-473138d8c376'
--and s.count = s.max_streak
and s.length >= 5
order by s.length desc