# Workshop 2: Streaming with SQL for Data Engineering Zoomcamp 2024 
## Homework

### Question 1
Create a materialized view to compute the average, min and max trip time between each taxi zone.

Note that we consider the do not consider a->b and b->a as the same trip pair. So as an example, you would consider the following trip pairs as different pairs:

- Yorkville East -> Steinway
- Steinway -> Yorkville East

From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the dynamic filter pattern for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.

**Options:**

- Yorkville East, Steinway
- Murray Hill, Midwood
- East Flatbush/Farragut, East Harlem North
- Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. A -> B and B -> A are considered different trips. This applies to subsequent questions as well.

**Procedure and Answer**

I created a Materializez View extracting the avg, min and max values of the difference betweent the pickup and drop off datetime.

After that I just queried the MV in order to order the results by avg trip time.


``` sql
-- Creating the MV
CREATE MATERIALIZED VIEW taxi_zone_trip_times AS
SELECT 
    taxi_zone.zone as PU_zone,
    taxi_zone_1.zone as DO_zone,
    avg(EXTRACT(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime)) as avg_trip_time,
    min(EXTRACT(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime)) as min_trip_time,
    max(EXTRACT(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime)) as max_trip_time    
from trip_data
JOIN taxi_zone ON trip_data.pulocationid = taxi_zone.location_id
JOIN taxi_zone as taxi_zone_1 ON trip_data.dolocationid = taxi_zone_1.location_id
GROUP BY taxi_zone.zone, taxi_zone_1.zone;

-- Querying the MV
SELECT * 
from taxi_zone_trip_times
order by avg_trip_time desc
limit 10;
```

Based on this the answer is: **Option 1: Yorkville East, Steinway**

### Question 2
Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

**Options:**
- 5
- 3
- 10
- 1

**Procedure and Answer**

In this case a count(*) was included on the MV, so it could be inclued on the query results

```sql
CREATE MATERIALIZED VIEW taxi_zone_trip_times_2 AS
SELECT 
    count(*) as trips,
    taxi_zone.zone as PU_zone,
    taxi_zone_1.zone as DO_zone,
    avg(EXTRACT(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime)) as avg_trip_time,
    min(EXTRACT(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime)) as min_trip_time,
    max(EXTRACT(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime)) as max_trip_time
from trip_data
JOIN taxi_zone ON trip_data.pulocationid = taxi_zone.location_id
JOIN taxi_zone as taxi_zone_1 ON trip_data.dolocationid = taxi_zone_1.location_id
GROUP BY taxi_zone.zone, taxi_zone_1.zone;

SELECT * 
from taxi_zone_trip_times_2
order by avg_trip_time desc
limit 10;
```

Based on this the answer is: **Option 4: 1**

### Question 3
From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 17:00:00, then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use dynamic filter pattern to create a filter condition based on the latest pickup time.

NOTE: For this question 17 hours was picked to ensure we have enough data to work with.

**Options:**

- Clinton East, Upper East Side North, Penn Station
- LaGuardia Airport, Lincoln Square East, JFK Airport
- Midtown Center, Upper East Side South, Upper East Side North
- LaGuardia Airport, Midtown Center, Upper East Side North

**Procedure and Answer**

For this case, I used a filter based on a datetime 17 hours before the max pickup datetime. After that counted the number of records in this period, using the following code:

```sql
WITH t AS (
    SELECT 
        MAX(tpep_pickup_datetime) - interval '17 hours' as before_latest_putime
    from trip_data
)
SELECT count(*) as trips, taxi_zone.Zone as taxi_zone
FROM t, trip_data
JOIN taxi_zone
    ON trip_data.PULocationID = taxi_zone.location_id
WHERE 
    trip_data.tpep_pickup_datetime >= t.before_latest_putime
GROUP BY taxi_zone.Zone
order by trips desc;

```

Based on this, the asnwer is: **Option 2: LaGuardia Airport, Lincoln Square East, JFK Airport**