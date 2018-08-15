
-- Get Bad Performaing Queries : Will provide if killed after putting QMR
with full_query_text as 
(
SELECT query,
       LISTAGG(TEXT) WITHIN
     GROUP(ORDER BY SEQUENCE) AS full_query_text
FROM stl_querytext
WHERE SEQUENCE< 100
GROUP BY query)


select sq.starttime,
sqms.userid,
sqms.query,
swra."rule",
sqms.query_cpu_time,
sqms.query_blocks_read,
sqms.query_execution_time,
sqms.query_cpu_usage_percent,
sqms.query_temp_blocks_to_disk,
sqms.segment_execution_time,
sqms.cpu_skew,io_skew,
sqms.scan_row_count,
sqms.join_row_count,
sqms.nested_loop_join_row_count,
sqms.return_row_count,
sqms.spectrum_scan_row_count,
sqms.spectrum_scan_size_mb,
sq.pid,
replace(fqt.full_query_text,'\\n',' ') as full_query_text
from full_query_text fqt 
inner join SVL_QUERY_METRICS_SUMMARY sqms 
on sqms.query=fqt.query
Inner Join stl_query sq
on sq.query=sqms.query
Left Join STL_WLM_RULE_ACTION swra
on swra.query=sq.query
where return_row_count >1000000
and sqms.userid=1
order by sq.starttime desc
limit 100
