with files_stats as (
    select min(filesize)/1048576 as lowerBound,
           max(filesize)/1048576 as upperBound
	from files
),
histogram as (
	select width_bucket(f.filesize/1048576, fs.lowerBound, fs.upperBound, 400) as bucket,
	min(f.filesize)/1048576 as lowerBound, max(f.filesize)/1048576 as upperBound, count(*) as freq
    from files f, files_stats fs
	group by bucket
	order by bucket
)
select bucket, lowerBound, upperBound, freq
from histogram;
