# HomeWork 1
## Docker / Terraform

### Question 1. Knowing docker tags

Run the command to get information on Docker
  docker --help

Now run the command to get help on the "docker build" command:
  docker build --help

Do the same for "docker run".
Which tag has the following text? - Automatically remove the container when it exits
--delete
--rc
--rmc
--rm

**Answer**

I executed the following command:

    docker run --help|grep Automatically
  
    --rm                             Automatically remove the container

So the answer is **--rm**

### Question 2. Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).
What is version of the package wheel ?
0.42.0
1.0.0
23.0.1
58.1.0

**Answer**

I executed the following commands:

    $ docker run -it --entrypoint=bash python:3.9
  
    root@4db51e6f5d64:/# pip list |grep wheel
  
    wheel      0.42.0

So the answer if **0.42.0**

### Question 3. Count records

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

15767
15612
15859
89009

**Answer**

After loading the data on the database, I executed the following query:

    select 
    	lpep_dropoff_datetime::date,
    	lpep_pickup_datetime::date,
    	count(1)
    from green_taxi_data
    WHERE 
    	lpep_dropoff_datetime::date = '2019-09-18' and
    	lpep_pickup_datetime::date = '2019-09-18'
    group by
    	lpep_dropoff_datetime::date,
    	lpep_pickup_datetime::date

The result of the query and the answer to the question is **15612**

### Question 4. Largest trip for each day
Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
2019-09-18
2019-09-16
2019-09-26
2019-09-21

**Answer**
The query I used in this case, was the following one:

    select lpep_pickup_datetime,  max(trip_distance)
    from green_taxi_data
    group by
    	lpep_pickup_datetime
    order by max desc;

The result of the query was **2019-09-26**

### Question 5. Three biggest pick up Boroughs
Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
"Brooklyn" "Manhattan" "Queens"
"Bronx" "Brooklyn" "Manhattan"
"Bronx" "Manhattan" "Queens"
"Brooklyn" "Queens" "Staten Island"

**Answer**
The query I used in this case, was the following one:

      select	
      	"Borough",
      	sum(total_amount) as total_amount
      from 
      	green_taxi_data t,
      	geen_zones z
      where 
      	t.lpep_pickup_datetime::date = '2019-09-18' and
      	t."PULocationID" = z."LocationID"
      group by z."Borough"
      order by total_amount desc

The result I got was the following order: **"Brooklyn" "Manhattan" "Queens"**

### Question 6. Largest tip
For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
Note: it's not a typo, it's tip , not trip
Central Park
Jamaica
JFK Airport
Long Island City/Queens Plaza

**Answer**
In this case I executed two queries:
1. The first one was to the the LocationID for Astoria, so i could use it in the final query.

        select
        	"PULocationID"
        from 
        	green_taxi_data t,
        	geen_zones z
        where
        	t."PULocationID" = z."LocationID" and
        	z."Zone" = 'Astoria'
        limit 1

The resulting PULocationID for Astoria is 7.

2. Then I ran a second Query to the the total tip amount:
   
        select
        	zdo."Zone",
        	SUM(tip_amount) AS "total_tip"
        from 
        	green_taxi_data t,
        	geen_zones zdo
        where
        	t."PULocationID" = 7 and
        	t."DOLocationID" = zdo."LocationID" and
        	t.lpep_pickup_datetime::date >= '2019-09-01' AND 
        	t.lpep_pickup_datetime::date <= '2019-09-30'
        group by 
        	zdo."Zone"
        order by 
        	total_tip desc
         limit 1

The result was:
Zone	total_tip
Astoria	1617.59

So my answer is **Astoria**. Not quite sure what am I doing wrong in this case.

   
   






