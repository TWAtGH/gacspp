with files_stats as (
    select min(filesize)/1048576 as min,
           max(filesize)/1048576 as max
	from files
),
histogram as (
	select width_bucket(filesize/1048576, min, max, 200) as bucket,
          int4range(min(filesize)/1048576, max(filesize)/1048576, '[]') as range,
          count(*) as freq
     from files, files_stats
	 group by bucket
	 order by bucket
)
select bucket, range, freq
from histogram;
