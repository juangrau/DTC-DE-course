# Week 3 Homework

## BigQuery

### Question 1:

Question 1: What is count of records for the 2022 Green Taxi Data??

- 65,623,481
- 840,402
- 1,936,423
- 253,647

**Answer**

The number of records is 840,402


### Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.  What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

**Answer**

The estimated amount of data would be 0 MB for the External Table and 6.41MB for the Materialized Table

### Question 3:

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622

**Answer**

There are 1,622 records that have a fare_amount of 0


## Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

**Answer**

The best strategy would be to partition by lpep_pickup_datetime and Cluster on PUlocationID

### Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)  

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?  

Choose the answer which most closely matches.  

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

**Answer**

The estimated bytes to be processed using non-partitioned and the partitioned table are respectively 12.82 MB and 1.12 MB

Question 6:

Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

**Answer**

As it is an3 external table, the data is stored in the GCP Bucket


### Question 7:

It is best practice in Big Query to always cluster your data:

- True
- False

**Answer**

The answer is False. The benefits of clustering will be appreciated depending on the nature of the data and the queries.


### (Bonus: Not worth points) Question 8:

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Answer**

0 Bytes. This is because BigQuery maintains metadata about each table, including the row count. As the query wont be actually performed then the estimated byte size is 0.