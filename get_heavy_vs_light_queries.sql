--Heavy Queries vs Light Queries :


select start_date,
SUM(case when query_usage='Heavy' then 1 end) as Heavy_count,
SUM(case when wait_state=1 then 1 end) as queue,
SUM(case when wait_state=0 then 1 end) as running
from (
Select distinct a.query,
'Heavy' as query_usage ,
case when (total_queue_time/1000000.0) =0 then 0 else 1 end as wait_state ,
cast((trunc(starttime)||' '|| extract(hour from starttime))||':00:00' as timestamp) as start_date
from 
stl_query q , STL_ALERT_EVENT_LOG a,STL_WLM_QUERY sq
where q.query=a.query
and sq.query=q.query 

union all


Select distinct q.query,
'Light' as query_usage ,
case when (total_queue_time/1000000.0) =0 then 0 else 1 end as wait_state ,
cast((trunc(starttime)||' '|| extract(hour from starttime))||':00:00' as timestamp) as start_date
from 
stl_query q left join STL_ALERT_EVENT_LOG a
on q.query=a.query
inner join STL_WLM_QUERY sq
on sq.query=q.query
where a.query is null
)
group by start_date