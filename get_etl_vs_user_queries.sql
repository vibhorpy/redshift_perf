--ETL,Reporting Queries by User Name :

--Userid 100 has both ETL and OBIEE Running. To differentiate them used 2 unions. --User 102,101 are ETL Users , 1,103,104,115,105,109 are reporting users

with etl_xid as (
select distinct xid from stl_query
where userid=100
and substring(lower(trim(querytxt)),1,6) in('insert' ,'copy n','vacuum','update','delete','copy a')
)
select start_date,
t.query,
query_type,
case when (total_queue_time/1000000.0) =0 then 0 else 1 end as wait_state,
cast(usename as varchar) as username from
(
select cast((trunc(starttime)||' '|| extract(hour from starttime))||':00:00' as timestamp) as start_date,query,'etl' as query_type,userid from stl_query , etl_xid
where stl_query.xid=etl_xid.xid 
and userid=100

union all

select cast((trunc(starttime)||' '|| extract(hour from starttime))||':00:00' as timestamp) as start_date,query,'user' as query_type,userid from stl_query left join etl_xid
on stl_query.xid=etl_xid.xid 
where userid=100
and etl_xid.xid is null 

union all

select cast((trunc(starttime)||' '|| extract(hour from starttime))||':00:00' as timestamp) as start_date,query,'etl' as query_type,userid from stl_query
where userid in(102,101)

union all

select cast((trunc(starttime)||' '|| extract(hour from starttime))||':00:00' as timestamp) as start_date,query,'user' as query_type,userid from stl_query
where userid in(1,103,104,115,105,109)
) t,
STL_WLM_QUERY sq,pg_user
where sq.query=t.query
and pg_user.usesysid=t.userid

