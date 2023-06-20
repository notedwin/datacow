-- query to find the time spent in vscode per file
-- SELECT CAST(datastr::json->'file' AS varchar), SUM(duration)/60 as minutes_spent FROM eventmodel where bucket_id = 5 GROUP BY 1 ORDER BY 2 desc


-- query to find the time spent in vscode per file
SELECT to_char(timestamp::timestamp, 'YYYY-MM'), (SUM(duration)/60)::bigint as minutes_spent  FROM eventmodel WHERE bucket_id = 5 GROUP BY 1 ORDER BY 2 desc


SELECT to_char(timestamp::timestamp, 'YYYY-WW'), (SUM(duration)/3600)::bigint as minutes_spent  FROM eventmodel WHERE bucket_id = 5 GROUP BY 1 ORDER BY 1 desc


-- time spent on various websites
SELECT substring(CAST(datastr::json->'url' AS varchar) from '(?:.*://)?(?:www\.)?([^/?]*)'), SUM(duration)/360 as minutes_spent FROM eventmodel where bucket_id = 6 GROUP BY 1 ORDER BY 2 desc


-- select * from pg_catalog.pg_stat_subscription;
-- ALTER SUBSCRIPTION logs DISABLE;