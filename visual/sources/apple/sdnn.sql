SELECT startdate::DATE as date,
    AVG(value::float) as sdnn
FROM "HeartRateVariabilitySDNN"
GROUP BY 1