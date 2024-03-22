---
title: Top Level Eggwin metrics
source: postgres.contributions
---

_how can we understand anything without measuring it?_

## Health Metrics

### Rolling Weekly Average Steps

```sql steps
SELECT
  date,
  AVG(steps::FLOAT) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_weekly_avg
FROM steps
WHERE date > current_date - INTERVAL 90 DAY
```

```sql historical_steps
-- get 6 month and 1 year averages
SELECT
    AVG(steps) as avg_6
FROM steps
WHERE date > current_date - INTERVAL 6 MONTH
```

<LineChart data={steps} x=date y=rolling_weekly_avg yAxisTitle="Steps">
    <ReferenceArea yMin=0 yMax=5000 color=red label="Lame"/>
    <ReferenceArea yMin=5000 yMax=10000 color=yellow label="Good"/>
    <ReferenceArea yMin=10000 yMax=20000 color=green label="Great"/>
    <ReferenceLine data={historical_steps} y=avg_6 label="6 year min" labelPosition="belowStart" color="purple"/>
</LineChart>

### Rolling Heart Rate for past 90 days

```sql rolling_hr
SELECT
    *
FROM rolling_hr
where time > current_date - INTERVAL 90 DAY
```

```sql avg_hr
SELECT
    AVG(avg_) as avg_
FROM ${rolling_hr}
```

<LineChart data={rolling_hr} x=time y=avg_ yAxisTitle="Heart Rate" yMin=50 yMax=75>
    <ReferenceArea yMin=50 yMax=57 color=green label="Great"/>
    <ReferenceArea yMin=57 yMax=65 color=yellow label="Good"/>
    <ReferenceArea yMin=65 yMax=100 color=red label="Bad"/>
    <ReferenceLine data={avg_hr} y=avg_ label="historical average" labelPosition="belowStart" color="purple"/>
</LineChart>

### Exercise Minutes

```sql exercise
SELECT
date,
least(mins_active, 150) as mins_active
FROM exercise
WHERE mins_active > 10
```

> TODO

## Internet Stats

```sql coding
SELECT * FROM hours
WHERE hours_coding > 0.25
AND date > current_date - INTERVAL '90 days'
```

```sql total_hours
SELECT
    SUM(hours_coding) as total_hours
FROM hours
WHERE date > current_date - INTERVAL 90 DAY
```

```sql previous_year
SELECT
    SUM(hours_coding) as previous_year
FROM hours
WHERE date > current_date - INTERVAL 1 YEAR
```

```sql daily_need
SELECT
    previous_year / 365 as daily_need
FROM ${previous_year}
```

<BigValue 
  data={total_hours}
  value=total_hours
  title="Hours Coding in past quarter"
/>

<BigValue 
  data={previous_year}
  value=previous_year
  title="Hours Coding in past year"
/>

<BigValue 
  data={daily_need}
  value=daily_need
  title="Daily needed to match last year"
/>

<BubbleChart
data={coding}
x=date
y=hours_coding
size=hours_coding
/>

<!-- <CalendarHeatmap
data={coding}
date=date
value=hours_coding
title="Hours Coding Heatmap"
subtitle="hours spent coding"
yearLabel=false,
/> -->

```sql contributions
SELECT
    date,
    least(contributions, 5) as contributions
FROM contributions
WHERE contributions > 0
AND date > current_date - INTERVAL 90 DAY
```

<CalendarHeatmap
data={contributions}
date=date
value=contributions
title="Calendar Heatmap for Github Contributions"
subtitle="daily contributions",
yearLabel=false,
colorPalette={["#9be9a8",
"#40c463",
"#30a14e",
"#216e39"]}
/>

### Weighted Rolling Request Count for past 90 days

```sql cf
SELECT time,
SUM(requests * weight) OVER (ORDER BY time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(weight) OVER (ORDER BY time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as weighted_rolling_avg
FROM (
    SELECT
    date::date as time,
    SUM(requests) as requests,
    COUNT(*) as weight
    FROM cloudflare
    WHERE date > current_date - INTERVAL 90 DAY
    GROUP BY 1
) as cf
```

```sql historical_cf
SELECT
    MIN(requests) as min_,
    AVG(requests) as avg_,
    MAX(requests) as max_
FROM cloudflare
WHERE date > current_date - INTERVAL 6 MONTH
```

<!-- ```sql
SELECT
SUM(CachedBytes)/SUM(Bytes) as cached_percentage
FROM cloudflare_analytics
``` -->

<LineChart data={cf} x=time y=weighted_rolling_avg yAxisTitle="Requests">
    <ReferenceLine data={historical_cf} y=avg_ label="6 month average" labelPosition="belowStart" color="yellow"/>
    <ReferenceLine data={historical_cf} y=min_ label="6 month min" labelPosition="belowStart" color="red"/>
    <ReferenceLine data={historical_cf} y=max_ label="6 month max" labelPosition="belowStart" color="green"/>
</LineChart>
