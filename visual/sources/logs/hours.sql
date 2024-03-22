SELECT timestamp::date as date,
    SUM(duration) / 3600 as hours_coding
FROM eventmodel
WHERE bucket_id IN (1, 5, 6, 7, 9, 10, 15, 16)
    AND datastr::json->>'app' = 'Code'
    AND timestamp::date >= '2023-01-01'
GROUP BY 1