SELECT startdate::DATE as date,
    SUM(value::int) as steps
FROM "StepCount"
GROUP BY 1