# Module 5 Homework - Spark 

## Question 1:
### Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?


```python
import pyspark
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    

    24/03/03 21:33:52 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    


```python
spark.version
```




    '3.3.2'



## Question 2:
### FHV October 2019

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 1MB
- 6MB
- 25MB
- 87MB


```python
# Download the file
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-10.parquet
```

    --2024-03-03 21:34:07--  https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-10.parquet
    Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 65.8.245.178, 65.8.245.51, 65.8.245.171, ...
    Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|65.8.245.178|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 18344035 (17M) [application/x-www-form-urlencoded]
    Saving to: ‘fhv_tripdata_2019-10.parquet.1’
    
    fhv_tripdata_2019-1 100%[===================>]  17.49M  55.9MB/s    in 0.3s    
    
    2024-03-03 21:34:08 (55.9 MB/s) - ‘fhv_tripdata_2019-10.parquet.1’ saved [18344035/18344035]
    
    


```python
df = spark.read \
    .option("header", "true") \
    .parquet('./fhv_tripdata_2019-10.parquet')
```


```python
df.schema
```




    StructType([StructField('dispatching_base_num', StringType(), True), StructField('pickup_datetime', TimestampType(), True), StructField('dropOff_datetime', TimestampType(), True), StructField('PUlocationID', DoubleType(), True), StructField('DOlocationID', DoubleType(), True), StructField('SR_Flag', IntegerType(), True), StructField('Affiliated_base_number', StringType(), True)])




```python
df_repartitioned = df.repartition(6)
```


```python
df_repartitioned.write.parquet('data/pq/fhv/', mode='overwrite')
```

                                                                                    


```python
!ls -lh ./data/pq/fhv
```

    total 39M
    -rw-r--r-- 1 juangrau juangrau    0 Mar  3 21:35 _SUCCESS
    -rw-r--r-- 1 juangrau juangrau 6.4M Mar  3 21:35 part-00000-f76ff9dc-aea1-454a-b926-93405a2d48da-c000.snappy.parquet
    -rw-r--r-- 1 juangrau juangrau 6.4M Mar  3 21:35 part-00001-f76ff9dc-aea1-454a-b926-93405a2d48da-c000.snappy.parquet
    -rw-r--r-- 1 juangrau juangrau 6.4M Mar  3 21:35 part-00002-f76ff9dc-aea1-454a-b926-93405a2d48da-c000.snappy.parquet
    -rw-r--r-- 1 juangrau juangrau 6.4M Mar  3 21:35 part-00003-f76ff9dc-aea1-454a-b926-93405a2d48da-c000.snappy.parquet
    -rw-r--r-- 1 juangrau juangrau 6.4M Mar  3 21:35 part-00004-f76ff9dc-aea1-454a-b926-93405a2d48da-c000.snappy.parquet
    -rw-r--r-- 1 juangrau juangrau 6.4M Mar  3 21:35 part-00005-f76ff9dc-aea1-454a-b926-93405a2d48da-c000.snappy.parquet
    

Answer: 6M

## Question 3:
### Count records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 108,164
- 12,856
- 452,470
- 62,610


```python
df_repartitioned.columns
```




    ['dispatching_base_num',
     'pickup_datetime',
     'dropOff_datetime',
     'PUlocationID',
     'DOlocationID',
     'SR_Flag',
     'Affiliated_base_number']




```python
from pyspark.sql import functions as F
```


```python
df_with_pu_date = df_repartitioned \
    .withColumn('pickup_date', F.to_date(df_repartitioned['pickup_datetime']))
```


```python
df_with_pu_date.filter(df_with_pu_date['pickup_date'] == '2019-10-15').count()
```




    62629




```python
import pandas as pd
from datetime import datetime
```


```python
tmpdf = pd.read_parquet('./fhv_tripdata_2019-10.parquet')
```


```python
tmpdf['pu_date'] = tmpdf['pickup_datetime'].dt.day
```


```python
tmpdf.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>dispatching_base_num</th>
      <th>pickup_datetime</th>
      <th>dropOff_datetime</th>
      <th>PUlocationID</th>
      <th>DOlocationID</th>
      <th>SR_Flag</th>
      <th>Affiliated_base_number</th>
      <th>pu_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>B00009</td>
      <td>2019-10-01 00:23:00</td>
      <td>2019-10-01 00:35:00</td>
      <td>264.0</td>
      <td>264.0</td>
      <td>None</td>
      <td>B00009</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B00013</td>
      <td>2019-10-01 00:11:29</td>
      <td>2019-10-01 00:13:22</td>
      <td>264.0</td>
      <td>264.0</td>
      <td>None</td>
      <td>B00013</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>B00014</td>
      <td>2019-10-01 00:11:43</td>
      <td>2019-10-01 00:37:20</td>
      <td>264.0</td>
      <td>264.0</td>
      <td>None</td>
      <td>B00014</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B00014</td>
      <td>2019-10-01 00:56:29</td>
      <td>2019-10-01 00:57:47</td>
      <td>264.0</td>
      <td>264.0</td>
      <td>None</td>
      <td>B00014</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>B00014</td>
      <td>2019-10-01 00:23:09</td>
      <td>2019-10-01 00:28:27</td>
      <td>264.0</td>
      <td>264.0</td>
      <td>None</td>
      <td>B00014</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
len(tmpdf[tmpdf['pu_date'] == 15])
```




    62629



## Question 4:
### Longest trip for each day

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours


```python
from pyspark.sql import types
```


```python
def delta_hours(pick_dt, drop_dt):
    time_delta = drop_dt - pick_dt
    return time_delta.total_seconds() / 3600
```


```python
delta_hours_udf = F.udf(delta_hours, returnType=types.DoubleType())
```


```python
df_trip_hours = df_with_pu_date \
    .withColumn("trip_hours", (F.col("dropOff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 3600) \
    .select('trip_hours', 'pickup_datetime', 'dropOff_datetime')
```


```python
df_trip_hours \
    .orderBy(df_trip_hours['trip_hours'].desc()) \
    .take(1)
```

                                                                                    




    [Row(trip_hours=631152.5, pickup_datetime=datetime.datetime(2019, 10, 11, 18, 0), dropOff_datetime=datetime.datetime(2091, 10, 11, 18, 30))]



Answer: 631152.5

## Question 5:
### User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

Answer: 4040

## Question 6:
### Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark
Zone Data

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?

- East Chelsea
- Jamaica Bay
- Union Sq
- Crown Heights North


```python
# get the Zone data
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

    --2024-03-03 22:46:17--  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
    Resolving github.com (github.com)... 140.82.112.3
    Connecting to github.com (github.com)|140.82.112.3|:443... connected.
    HTTP request sent, awaiting response... 302 Found
    Location: https://objects.githubusercontent.com/github-production-release-asset-2e65be/513814948/5a2cc2f5-b4cd-4584-9c62-a6ea97ed0e6a?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20240303%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240303T224617Z&X-Amz-Expires=300&X-Amz-Signature=3a9c82abe930e94157beb01dbade77ee63cd5ace0a08bbc8a5563ebfce3f0b0f&X-Amz-SignedHeaders=host&actor_id=0&key_id=0&repo_id=513814948&response-content-disposition=attachment%3B%20filename%3Dtaxi_zone_lookup.csv&response-content-type=application%2Foctet-stream [following]
    --2024-03-03 22:46:17--  https://objects.githubusercontent.com/github-production-release-asset-2e65be/513814948/5a2cc2f5-b4cd-4584-9c62-a6ea97ed0e6a?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20240303%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240303T224617Z&X-Amz-Expires=300&X-Amz-Signature=3a9c82abe930e94157beb01dbade77ee63cd5ace0a08bbc8a5563ebfce3f0b0f&X-Amz-SignedHeaders=host&actor_id=0&key_id=0&repo_id=513814948&response-content-disposition=attachment%3B%20filename%3Dtaxi_zone_lookup.csv&response-content-type=application%2Foctet-stream
    Resolving objects.githubusercontent.com (objects.githubusercontent.com)... 185.199.109.133, 185.199.110.133, 185.199.111.133, ...
    Connecting to objects.githubusercontent.com (objects.githubusercontent.com)|185.199.109.133|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 12322 (12K) [application/octet-stream]
    Saving to: ‘taxi_zone_lookup.csv’
    
    taxi_zone_lookup.cs 100%[===================>]  12.03K  --.-KB/s    in 0s      
    
    2024-03-03 22:46:17 (26.2 MB/s) - ‘taxi_zone_lookup.csv’ saved [12322/12322]
    
    


```python
df_zones = spark.read.csv('./taxi_zone_lookup.csv', header=True)
```


```python
df_zones.show()
```

    +----------+-------------+--------------------+------------+
    |LocationID|      Borough|                Zone|service_zone|
    +----------+-------------+--------------------+------------+
    |         1|          EWR|      Newark Airport|         EWR|
    |         2|       Queens|         Jamaica Bay|   Boro Zone|
    |         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
    |         4|    Manhattan|       Alphabet City| Yellow Zone|
    |         5|Staten Island|       Arden Heights|   Boro Zone|
    |         6|Staten Island|Arrochar/Fort Wad...|   Boro Zone|
    |         7|       Queens|             Astoria|   Boro Zone|
    |         8|       Queens|        Astoria Park|   Boro Zone|
    |         9|       Queens|          Auburndale|   Boro Zone|
    |        10|       Queens|        Baisley Park|   Boro Zone|
    |        11|     Brooklyn|          Bath Beach|   Boro Zone|
    |        12|    Manhattan|        Battery Park| Yellow Zone|
    |        13|    Manhattan|   Battery Park City| Yellow Zone|
    |        14|     Brooklyn|           Bay Ridge|   Boro Zone|
    |        15|       Queens|Bay Terrace/Fort ...|   Boro Zone|
    |        16|       Queens|             Bayside|   Boro Zone|
    |        17|     Brooklyn|             Bedford|   Boro Zone|
    |        18|        Bronx|        Bedford Park|   Boro Zone|
    |        19|       Queens|           Bellerose|   Boro Zone|
    |        20|        Bronx|             Belmont|   Boro Zone|
    +----------+-------------+--------------------+------------+
    only showing top 20 rows
    
    


```python
df_zones \
    .select('LocationID', 'Borough', 'Zone') \
    .where(df_zones['LocationID']==264) \
    .show()
```

    +----------+-------+----+
    |LocationID|Borough|Zone|
    +----------+-------+----+
    |       264|Unknown|  NV|
    +----------+-------+----+
    
    


```python
df_with_pu_date.show()
```

    [Stage 39:===========================================>              (3 + 1) / 4]

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|pickup_date|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+
    |              B00310|2019-10-17 16:55:36|2019-10-17 16:59:14|       264.0|       208.0|   null|                B03017| 2019-10-17|
    |              B01626|2019-10-05 02:25:14|2019-10-05 02:43:43|       264.0|       197.0|   null|                B01626| 2019-10-05|
    |              B00789|2019-10-03 22:43:16|2019-10-03 22:52:21|       264.0|       264.0|   null|                B00789| 2019-10-03|
    |              B00900|2019-10-09 10:59:32|2019-10-09 11:15:28|       264.0|       258.0|   null|                B00900| 2019-10-09|
    |              B02157|2019-10-18 18:19:11|2019-10-18 18:21:10|       264.0|       265.0|   null|                B02157| 2019-10-18|
    |              B03055|2019-10-18 13:55:58|2019-10-18 15:12:59|       264.0|       228.0|   null|                B03055| 2019-10-18|
    |              B02653|2019-10-04 10:10:17|2019-10-04 10:34:06|       264.0|       241.0|   null|                B02653| 2019-10-04|
    |              B03160|2019-10-06 14:28:00|2019-10-06 14:44:00|       155.0|       155.0|   null|                B02875| 2019-10-06|
    |              B02461|2019-10-18 16:04:44|2019-10-18 16:10:03|       264.0|       119.0|   null|                B02461| 2019-10-18|
    |              B02133|2019-10-11 11:07:26|2019-10-11 12:00:04|       264.0|       264.0|   null|                B01899| 2019-10-11|
    |              B00272|2019-10-18 21:50:14|2019-10-18 22:57:00|       264.0|       264.0|   null|                B00272| 2019-10-18|
    |              B01315|2019-10-22 22:23:57|2019-10-22 22:29:25|       264.0|       242.0|   null|                B01315| 2019-10-22|
    |              B03080|2019-10-12 07:01:57|2019-10-12 08:33:05|       264.0|       174.0|   null|                B03127| 2019-10-12|
    |              B02401|2019-10-15 08:15:19|2019-10-15 08:34:27|       264.0|       145.0|   null|                B02401| 2019-10-15|
    |              B00256|2019-10-14 18:47:40|2019-10-14 19:16:30|       264.0|       264.0|   null|                B00256| 2019-10-14|
    |              B01800|2019-10-07 10:31:00|2019-10-07 11:54:00|       264.0|       264.0|   null|                B01800| 2019-10-07|
    |              B00860|2019-10-15 09:43:22|2019-10-15 09:57:13|       264.0|       119.0|   null|                B00860| 2019-10-15|
    |              B02677|2019-10-05 09:08:21|2019-10-05 09:21:03|       264.0|        76.0|   null|                B02677| 2019-10-05|
    |              B03060|2019-10-04 15:45:54|2019-10-04 16:00:52|       264.0|       165.0|   null|                B03060| 2019-10-04|
    |              B02715|2019-10-14 15:10:09|2019-10-14 16:26:03|       265.0|       252.0|   null|                B02715| 2019-10-14|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+
    only showing top 20 rows
    
    

                                                                                    


```python
df_joined = df_with_pu_date.join(df_zones, df_with_pu_date.PUlocationID == df_zones.LocationID)
```


```python
df_joined.show()
```

    [Stage 42:===========================================>              (3 + 1) / 4]

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+----------+--------+--------------------+------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|pickup_date|LocationID| Borough|                Zone|service_zone|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+----------+--------+--------------------+------------+
    |              B00310|2019-10-17 16:55:36|2019-10-17 16:59:14|       264.0|       208.0|   null|                B03017| 2019-10-17|       264| Unknown|                  NV|         N/A|
    |              B01626|2019-10-05 02:25:14|2019-10-05 02:43:43|       264.0|       197.0|   null|                B01626| 2019-10-05|       264| Unknown|                  NV|         N/A|
    |              B00789|2019-10-03 22:43:16|2019-10-03 22:52:21|       264.0|       264.0|   null|                B00789| 2019-10-03|       264| Unknown|                  NV|         N/A|
    |              B00900|2019-10-09 10:59:32|2019-10-09 11:15:28|       264.0|       258.0|   null|                B00900| 2019-10-09|       264| Unknown|                  NV|         N/A|
    |              B02157|2019-10-18 18:19:11|2019-10-18 18:21:10|       264.0|       265.0|   null|                B02157| 2019-10-18|       264| Unknown|                  NV|         N/A|
    |              B03055|2019-10-18 13:55:58|2019-10-18 15:12:59|       264.0|       228.0|   null|                B03055| 2019-10-18|       264| Unknown|                  NV|         N/A|
    |              B02653|2019-10-04 10:10:17|2019-10-04 10:34:06|       264.0|       241.0|   null|                B02653| 2019-10-04|       264| Unknown|                  NV|         N/A|
    |              B03160|2019-10-06 14:28:00|2019-10-06 14:44:00|       155.0|       155.0|   null|                B02875| 2019-10-06|       155|Brooklyn|Marine Park/Mill ...|   Boro Zone|
    |              B02461|2019-10-18 16:04:44|2019-10-18 16:10:03|       264.0|       119.0|   null|                B02461| 2019-10-18|       264| Unknown|                  NV|         N/A|
    |              B02133|2019-10-11 11:07:26|2019-10-11 12:00:04|       264.0|       264.0|   null|                B01899| 2019-10-11|       264| Unknown|                  NV|         N/A|
    |              B00272|2019-10-18 21:50:14|2019-10-18 22:57:00|       264.0|       264.0|   null|                B00272| 2019-10-18|       264| Unknown|                  NV|         N/A|
    |              B01315|2019-10-22 22:23:57|2019-10-22 22:29:25|       264.0|       242.0|   null|                B01315| 2019-10-22|       264| Unknown|                  NV|         N/A|
    |              B03080|2019-10-12 07:01:57|2019-10-12 08:33:05|       264.0|       174.0|   null|                B03127| 2019-10-12|       264| Unknown|                  NV|         N/A|
    |              B02401|2019-10-15 08:15:19|2019-10-15 08:34:27|       264.0|       145.0|   null|                B02401| 2019-10-15|       264| Unknown|                  NV|         N/A|
    |              B00256|2019-10-14 18:47:40|2019-10-14 19:16:30|       264.0|       264.0|   null|                B00256| 2019-10-14|       264| Unknown|                  NV|         N/A|
    |              B01800|2019-10-07 10:31:00|2019-10-07 11:54:00|       264.0|       264.0|   null|                B01800| 2019-10-07|       264| Unknown|                  NV|         N/A|
    |              B00860|2019-10-15 09:43:22|2019-10-15 09:57:13|       264.0|       119.0|   null|                B00860| 2019-10-15|       264| Unknown|                  NV|         N/A|
    |              B02677|2019-10-05 09:08:21|2019-10-05 09:21:03|       264.0|        76.0|   null|                B02677| 2019-10-05|       264| Unknown|                  NV|         N/A|
    |              B03060|2019-10-04 15:45:54|2019-10-04 16:00:52|       264.0|       165.0|   null|                B03060| 2019-10-04|       264| Unknown|                  NV|         N/A|
    |              B02715|2019-10-14 15:10:09|2019-10-14 16:26:03|       265.0|       252.0|   null|                B02715| 2019-10-14|       265| Unknown|                  NA|         N/A|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+-----------+----------+--------+--------------------+------------+
    only showing top 20 rows
    
    

                                                                                    


```python
df_joined.registerTempTable('joined')
```

    /home/juangrau/spark/spark-3.3.2-bin-hadoop3/python/pyspark/sql/dataframe.py:229: FutureWarning: Deprecated in 2.0, use createOrReplaceTempView instead.
      warnings.warn("Deprecated in 2.0, use createOrReplaceTempView instead.", FutureWarning)
    


```python
df_zones_grouped = spark.sql("""
SELECT 
    Zone, COUNT(Zone) AS Count
FROM
    joined
GROUP BY
    Zone
ORDER BY
    Count ASC
""")
```


```python
df_zones_grouped.show()
```

    [Stage 74:===========================================>              (3 + 1) / 4]

    +--------------------+-----+
    |                Zone|Count|
    +--------------------+-----+
    |         Jamaica Bay|    1|
    |Governor's Island...|    2|
    | Green-Wood Cemetery|    5|
    |       Broad Channel|    8|
    |     Highbridge Park|   14|
    |        Battery Park|   15|
    |Saint Michaels Ce...|   23|
    |Breezy Point/Fort...|   25|
    |Marine Park/Floyd...|   26|
    |        Astoria Park|   29|
    |    Inwood Hill Park|   39|
    |       Willets Point|   47|
    |Forest Park/Highl...|   53|
    |  Brooklyn Navy Yard|   57|
    |        Crotona Park|   62|
    |        Country Club|   77|
    |     Freshkills Park|   89|
    |       Prospect Park|   98|
    |     Columbia Street|  105|
    |  South Williamsburg|  110|
    +--------------------+-----+
    only showing top 20 rows
    
    

                                                                                    

Answer: Jamaica Bay


```python

```


```python

```


```python
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet
```

    --2024-02-26 21:22:31--  https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet
    Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 65.8.245.51, 65.8.245.50, 65.8.245.178, ...
    Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|65.8.245.51|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 308924937 (295M) [application/x-www-form-urlencoded]
    Saving to: ‘fhvhv_tripdata_2021-01.parquet’
    
    fhvhv_tripdata_2021 100%[===================>] 294.61M  65.8MB/s    in 4.6s    
    
    2024-02-26 21:22:35 (63.7 MB/s) - ‘fhvhv_tripdata_2021-01.parquet’ saved [308924937/308924937]
    
    


```python
import pandas as pd
```


```python
tmpdf = pd.read_parquet('./fhvhv_tripdata_2021-01.parquet')
tmpdf.to_csv('./fhvhv_tripdata_2021-01.csv', index=False)
```


```python
df = spark.read \
    .option("header", "true") \
    .parquet('./fhvhv_tripdata_2021-01.parquet')
```

                                                                                    


```python
df.show()
```

    [Stage 2:=======================================>                   (2 + 1) / 3]

    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    |hvfhs_license_num|dispatching_base_num|originating_base_num|   request_datetime|  on_scene_datetime|    pickup_datetime|   dropoff_datetime|PULocationID|DOLocationID|trip_miles|trip_time|base_passenger_fare|tolls| bcf|sales_tax|congestion_surcharge|airport_fee|tips|driver_pay|shared_request_flag|shared_match_flag|access_a_ride_flag|wav_request_flag|wav_match_flag|
    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    |           HV0003|              B02682|              B02682|2021-01-01 00:28:09|2021-01-01 00:31:42|2021-01-01 00:33:44|2021-01-01 00:49:07|         230|         166|      5.26|      923|              22.28|  0.0|0.67|     1.98|                2.75|       null| 0.0|     14.99|                  N|                N|                  |               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:45:56|2021-01-01 00:55:19|2021-01-01 00:55:19|2021-01-01 01:18:21|         152|         167|      3.65|     1382|              18.36|  0.0|0.55|     1.63|                 0.0|       null| 0.0|     17.06|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:21:15|2021-01-01 00:22:41|2021-01-01 00:23:56|2021-01-01 00:38:05|         233|         142|      3.51|      849|              14.05|  0.0|0.48|     1.25|                2.75|       null|0.94|     12.98|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:39:12|2021-01-01 00:42:37|2021-01-01 00:42:51|2021-01-01 00:45:50|         142|         143|      0.74|      179|               7.91|  0.0|0.24|      0.7|                2.75|       null| 0.0|      7.41|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:46:11|2021-01-01 00:47:17|2021-01-01 00:48:14|2021-01-01 01:08:42|         143|          78|       9.2|     1228|              27.11|  0.0|0.81|     2.41|                2.75|       null| 0.0|     22.44|                  N|                N|                  |               N|             N|
    |           HV0005|              B02510|                null|2021-01-01 00:04:00|               null|2021-01-01 00:06:59|2021-01-01 00:43:01|          88|          42|     9.725|     2162|              28.11|  0.0|0.84|     2.49|                2.75|       null| 0.0|      28.9|                  N|                N|                 N|               N|             N|
    |           HV0005|              B02510|                null|2021-01-01 00:40:06|               null|2021-01-01 00:50:00|2021-01-01 01:04:57|          42|         151|     2.469|      897|              25.03|  0.0|0.75|     2.22|                 0.0|       null| 0.0|     15.01|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:10:36|2021-01-01 00:12:28|2021-01-01 00:14:30|2021-01-01 00:50:27|          71|         226|     13.53|     2157|              29.67|  0.0|1.04|     3.08|                 0.0|       null| 0.0|      34.2|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-01 00:21:17|2021-01-01 00:22:25|2021-01-01 00:22:54|2021-01-01 00:30:20|         112|         255|       1.6|      446|               6.89|  0.0|0.21|     0.61|                 0.0|       null| 0.0|      6.26|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-01 00:36:57|2021-01-01 00:38:09|2021-01-01 00:40:12|2021-01-01 00:53:31|         255|         232|       3.2|      800|              11.51|  0.0|0.53|     1.03|                2.75|       null|2.82|     10.99|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-01 00:53:31|2021-01-01 00:56:21|2021-01-01 00:56:45|2021-01-01 01:17:42|         232|         198|      5.74|     1257|              17.18|  0.0|0.52|     1.52|                2.75|       null| 0.0|     17.61|                  N|                N|                  |               N|             N|
    |           HV0003|              B02835|              B02835|2021-01-01 00:22:58|2021-01-01 00:27:01|2021-01-01 00:29:04|2021-01-01 00:36:27|         113|          48|       1.8|      443|               8.18|  0.0|0.25|     0.73|                2.75|       null| 0.0|      6.12|                  N|                N|                  |               N|             N|
    |           HV0003|              B02835|              B02835|2021-01-01 00:46:44|2021-01-01 00:47:49|2021-01-01 00:48:56|2021-01-01 00:59:12|         239|          75|       2.9|      616|               13.1|  0.0|0.45|     1.17|                2.75|       null|0.94|      8.77|                  N|                N|                  |               N|             N|
    |           HV0004|              B02800|                null|2021-01-01 00:12:50|               null|2021-01-01 00:15:24|2021-01-01 00:38:31|         181|         237|      9.66|     1387|              32.95|  0.0| 0.0|     2.34|                2.75|       null| 0.0|      21.1|                  N|                N|                 N|               N|             N|
    |           HV0004|              B02800|                null|2021-01-01 00:35:32|               null|2021-01-01 00:45:00|2021-01-01 01:06:45|         236|          68|      4.38|     1305|              22.91|  0.0| 0.0|     1.63|                2.75|       null|3.43|     15.82|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:10:22|2021-01-01 00:11:03|2021-01-01 00:11:53|2021-01-01 00:18:06|         256|         148|      2.03|      373|               7.84|  0.0|0.42|      0.7|                2.75|       null|2.82|      6.93|                  N|                N|                  |               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:25:00|2021-01-01 00:26:31|2021-01-01 00:28:31|2021-01-01 00:41:40|          79|          80|      3.08|      789|               13.2|  0.0| 0.4|     1.17|                2.75|       null| 0.0|     11.54|                  N|                N|                  |               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:44:56|2021-01-01 00:49:55|2021-01-01 00:50:49|2021-01-01 00:55:59|          17|         217|      1.17|      310|               7.91|  0.0|0.24|      0.7|                 0.0|       null| 0.0|      6.94|                  N|                N|                  |               N|             N|
    |           HV0005|              B02510|                null|2021-01-01 00:05:04|               null|2021-01-01 00:08:40|2021-01-01 00:39:39|          62|          29|    10.852|     1859|              31.18|  0.0|0.94|     2.77|                 0.0|       null| 0.0|     27.61|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02836|              B02836|2021-01-01 00:40:44|2021-01-01 00:53:34|2021-01-01 00:53:48|2021-01-01 01:11:40|          22|          22|      3.52|     1072|              28.67|  0.0|0.86|     2.54|                 0.0|       null| 0.0|     17.64|                  N|                N|                  |               N|             N|
    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    only showing top 20 rows
    
    

                                                                                    


```python
df.schema
```




    StructType([StructField('hvfhs_license_num', StringType(), True), StructField('dispatching_base_num', StringType(), True), StructField('originating_base_num', StringType(), True), StructField('request_datetime', TimestampType(), True), StructField('on_scene_datetime', TimestampType(), True), StructField('pickup_datetime', TimestampType(), True), StructField('dropoff_datetime', TimestampType(), True), StructField('PULocationID', LongType(), True), StructField('DOLocationID', LongType(), True), StructField('trip_miles', DoubleType(), True), StructField('trip_time', LongType(), True), StructField('base_passenger_fare', DoubleType(), True), StructField('tolls', DoubleType(), True), StructField('bcf', DoubleType(), True), StructField('sales_tax', DoubleType(), True), StructField('congestion_surcharge', DoubleType(), True), StructField('airport_fee', DoubleType(), True), StructField('tips', DoubleType(), True), StructField('driver_pay', DoubleType(), True), StructField('shared_request_flag', StringType(), True), StructField('shared_match_flag', StringType(), True), StructField('access_a_ride_flag', StringType(), True), StructField('wav_request_flag', StringType(), True), StructField('wav_match_flag', StringType(), True)])




```python
df.head(5)
```




    [Row(hvfhs_license_num='HV0003', dispatching_base_num='B02682', originating_base_num='B02682', request_datetime=datetime.datetime(2021, 1, 1, 0, 28, 9), on_scene_datetime=datetime.datetime(2021, 1, 1, 0, 31, 42), pickup_datetime=datetime.datetime(2021, 1, 1, 0, 33, 44), dropoff_datetime=datetime.datetime(2021, 1, 1, 0, 49, 7), PULocationID=230, DOLocationID=166, trip_miles=5.26, trip_time=923, base_passenger_fare=22.28, tolls=0.0, bcf=0.67, sales_tax=1.98, congestion_surcharge=2.75, airport_fee=None, tips=0.0, driver_pay=14.99, shared_request_flag='N', shared_match_flag='N', access_a_ride_flag=' ', wav_request_flag='N', wav_match_flag='N'),
     Row(hvfhs_license_num='HV0003', dispatching_base_num='B02682', originating_base_num='B02682', request_datetime=datetime.datetime(2021, 1, 1, 0, 45, 56), on_scene_datetime=datetime.datetime(2021, 1, 1, 0, 55, 19), pickup_datetime=datetime.datetime(2021, 1, 1, 0, 55, 19), dropoff_datetime=datetime.datetime(2021, 1, 1, 1, 18, 21), PULocationID=152, DOLocationID=167, trip_miles=3.65, trip_time=1382, base_passenger_fare=18.36, tolls=0.0, bcf=0.55, sales_tax=1.63, congestion_surcharge=0.0, airport_fee=None, tips=0.0, driver_pay=17.06, shared_request_flag='N', shared_match_flag='N', access_a_ride_flag=' ', wav_request_flag='N', wav_match_flag='N'),
     Row(hvfhs_license_num='HV0003', dispatching_base_num='B02764', originating_base_num='B02764', request_datetime=datetime.datetime(2021, 1, 1, 0, 21, 15), on_scene_datetime=datetime.datetime(2021, 1, 1, 0, 22, 41), pickup_datetime=datetime.datetime(2021, 1, 1, 0, 23, 56), dropoff_datetime=datetime.datetime(2021, 1, 1, 0, 38, 5), PULocationID=233, DOLocationID=142, trip_miles=3.51, trip_time=849, base_passenger_fare=14.05, tolls=0.0, bcf=0.48, sales_tax=1.25, congestion_surcharge=2.75, airport_fee=None, tips=0.94, driver_pay=12.98, shared_request_flag='N', shared_match_flag='N', access_a_ride_flag=' ', wav_request_flag='N', wav_match_flag='N'),
     Row(hvfhs_license_num='HV0003', dispatching_base_num='B02764', originating_base_num='B02764', request_datetime=datetime.datetime(2021, 1, 1, 0, 39, 12), on_scene_datetime=datetime.datetime(2021, 1, 1, 0, 42, 37), pickup_datetime=datetime.datetime(2021, 1, 1, 0, 42, 51), dropoff_datetime=datetime.datetime(2021, 1, 1, 0, 45, 50), PULocationID=142, DOLocationID=143, trip_miles=0.74, trip_time=179, base_passenger_fare=7.91, tolls=0.0, bcf=0.24, sales_tax=0.7, congestion_surcharge=2.75, airport_fee=None, tips=0.0, driver_pay=7.41, shared_request_flag='N', shared_match_flag='N', access_a_ride_flag=' ', wav_request_flag='N', wav_match_flag='N'),
     Row(hvfhs_license_num='HV0003', dispatching_base_num='B02764', originating_base_num='B02764', request_datetime=datetime.datetime(2021, 1, 1, 0, 46, 11), on_scene_datetime=datetime.datetime(2021, 1, 1, 0, 47, 17), pickup_datetime=datetime.datetime(2021, 1, 1, 0, 48, 14), dropoff_datetime=datetime.datetime(2021, 1, 1, 1, 8, 42), PULocationID=143, DOLocationID=78, trip_miles=9.2, trip_time=1228, base_passenger_fare=27.11, tolls=0.0, bcf=0.81, sales_tax=2.41, congestion_surcharge=2.75, airport_fee=None, tips=0.0, driver_pay=22.44, shared_request_flag='N', shared_match_flag='N', access_a_ride_flag=' ', wav_request_flag='N', wav_match_flag='N')]




```python
df.repartition(24)
```




    DataFrame[hvfhs_license_num: string, dispatching_base_num: string, originating_base_num: string, request_datetime: timestamp, on_scene_datetime: timestamp, pickup_datetime: timestamp, dropoff_datetime: timestamp, PULocationID: bigint, DOLocationID: bigint, trip_miles: double, trip_time: bigint, base_passenger_fare: double, tolls: double, bcf: double, sales_tax: double, congestion_surcharge: double, airport_fee: double, tips: double, driver_pay: double, shared_request_flag: string, shared_match_flag: string, access_a_ride_flag: string, wav_request_flag: string, wav_match_flag: string]




```python
df.write.parquet('./fhvhv/2021/01/')
```

                                                                                    


```python
df2 = spark.read.parquet('./fhvhv/2021/01/')
```


```python
for element in df2.schema:
    print(element)
```

    StructField('hvfhs_license_num', StringType(), True)
    StructField('dispatching_base_num', StringType(), True)
    StructField('originating_base_num', StringType(), True)
    StructField('request_datetime', TimestampType(), True)
    StructField('on_scene_datetime', TimestampType(), True)
    StructField('pickup_datetime', TimestampType(), True)
    StructField('dropoff_datetime', TimestampType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('trip_miles', DoubleType(), True)
    StructField('trip_time', LongType(), True)
    StructField('base_passenger_fare', DoubleType(), True)
    StructField('tolls', DoubleType(), True)
    StructField('bcf', DoubleType(), True)
    StructField('sales_tax', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
    StructField('tips', DoubleType(), True)
    StructField('driver_pay', DoubleType(), True)
    StructField('shared_request_flag', StringType(), True)
    StructField('shared_match_flag', StringType(), True)
    StructField('access_a_ride_flag', StringType(), True)
    StructField('wav_request_flag', StringType(), True)
    StructField('wav_match_flag', StringType(), True)
    

## Example of select


```python
!head -3 fhvhv_tripdata_2021-01.csv
```

    hvfhs_license_num,dispatching_base_num,originating_base_num,request_datetime,on_scene_datetime,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,trip_miles,trip_time,base_passenger_fare,tolls,bcf,sales_tax,congestion_surcharge,airport_fee,tips,driver_pay,shared_request_flag,shared_match_flag,access_a_ride_flag,wav_request_flag,wav_match_flag
    HV0003,B02682,B02682,2021-01-01 00:28:09,2021-01-01 00:31:42,2021-01-01 00:33:44,2021-01-01 00:49:07,230,166,5.26,923,22.28,0.0,0.67,1.98,2.75,,0.0,14.99,N,N, ,N,N
    HV0003,B02682,B02682,2021-01-01 00:45:56,2021-01-01 00:55:19,2021-01-01 00:55:19,2021-01-01 01:18:21,152,167,3.65,1382,18.36,0.0,0.55,1.63,0.0,,0.0,17.06,N,N, ,N,N
    


```python
# This command is lazy, this means it doesn't change anything until a show is requested
df2.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
    .filter(df2['hvfhs_license_num'] == 'HV0003') \
    .show()
```

    +-------------------+-------------------+------------+------------+
    |    pickup_datetime|   dropoff_datetime|PULocationID|DOLocationID|
    +-------------------+-------------------+------------+------------+
    |2021-01-01 00:33:44|2021-01-01 00:49:07|         230|         166|
    |2021-01-01 00:55:19|2021-01-01 01:18:21|         152|         167|
    |2021-01-01 00:23:56|2021-01-01 00:38:05|         233|         142|
    |2021-01-01 00:42:51|2021-01-01 00:45:50|         142|         143|
    |2021-01-01 00:48:14|2021-01-01 01:08:42|         143|          78|
    |2021-01-01 00:14:30|2021-01-01 00:50:27|          71|         226|
    |2021-01-01 00:22:54|2021-01-01 00:30:20|         112|         255|
    |2021-01-01 00:40:12|2021-01-01 00:53:31|         255|         232|
    |2021-01-01 00:56:45|2021-01-01 01:17:42|         232|         198|
    |2021-01-01 00:29:04|2021-01-01 00:36:27|         113|          48|
    |2021-01-01 00:48:56|2021-01-01 00:59:12|         239|          75|
    |2021-01-01 00:11:53|2021-01-01 00:18:06|         256|         148|
    |2021-01-01 00:28:31|2021-01-01 00:41:40|          79|          80|
    |2021-01-01 00:50:49|2021-01-01 00:55:59|          17|         217|
    |2021-01-01 00:53:48|2021-01-01 01:11:40|          22|          22|
    |2021-01-01 00:36:21|2021-01-01 00:57:41|         146|         129|
    |2021-01-01 00:14:21|2021-01-01 00:21:16|          37|         225|
    |2021-01-01 00:26:37|2021-01-01 01:08:20|         225|          47|
    |2021-01-01 00:19:11|2021-01-01 00:33:49|          81|          32|
    |2021-01-01 00:39:51|2021-01-01 00:58:51|          32|         126|
    +-------------------+-------------------+------------+------------+
    only showing top 20 rows
    
    

## sql functions with pyspark


```python
from pyspark.sql import functions as F
from pyspark.sql import types
```


```python
df2 \
    .withColumn('pickup_date', F.to_date(df2['pickup_datetime'])) \
    .withColumn('dropoff_date', F.to_date(df2['dropoff_datetime'])) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

    +-----------+------------+------------+------------+
    |pickup_date|dropoff_date|PULocationID|DOLocationID|
    +-----------+------------+------------+------------+
    | 2021-01-01|  2021-01-01|         230|         166|
    | 2021-01-01|  2021-01-01|         152|         167|
    | 2021-01-01|  2021-01-01|         233|         142|
    | 2021-01-01|  2021-01-01|         142|         143|
    | 2021-01-01|  2021-01-01|         143|          78|
    | 2021-01-01|  2021-01-01|          88|          42|
    | 2021-01-01|  2021-01-01|          42|         151|
    | 2021-01-01|  2021-01-01|          71|         226|
    | 2021-01-01|  2021-01-01|         112|         255|
    | 2021-01-01|  2021-01-01|         255|         232|
    | 2021-01-01|  2021-01-01|         232|         198|
    | 2021-01-01|  2021-01-01|         113|          48|
    | 2021-01-01|  2021-01-01|         239|          75|
    | 2021-01-01|  2021-01-01|         181|         237|
    | 2021-01-01|  2021-01-01|         236|          68|
    | 2021-01-01|  2021-01-01|         256|         148|
    | 2021-01-01|  2021-01-01|          79|          80|
    | 2021-01-01|  2021-01-01|          17|         217|
    | 2021-01-01|  2021-01-01|          62|          29|
    | 2021-01-01|  2021-01-01|          22|          22|
    +-----------+------------+------------+------------+
    only showing top 20 rows
    
    


```python
# defining an function
def crazy_stuff(base_num):
    '''
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'
    '''
    return 'hello'
```


```python
crazy_stuff('B01100')
```




    'hello'




```python
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
```


```python
df2 \
    .withColumn('pickup_date', F.to_date(df2['pickup_datetime'])) \
    .withColumn('dropoff_date', F.to_date(df2['dropoff_datetime'])) \
    .withColumn('base_num', crazy_stuff_udf(df2['dispatching_base_num'])) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

    +-----------+------------+------------+------------+
    |pickup_date|dropoff_date|PULocationID|DOLocationID|
    +-----------+------------+------------+------------+
    | 2021-01-01|  2021-01-01|         230|         166|
    | 2021-01-01|  2021-01-01|         152|         167|
    | 2021-01-01|  2021-01-01|         233|         142|
    | 2021-01-01|  2021-01-01|         142|         143|
    | 2021-01-01|  2021-01-01|         143|          78|
    | 2021-01-01|  2021-01-01|          88|          42|
    | 2021-01-01|  2021-01-01|          42|         151|
    | 2021-01-01|  2021-01-01|          71|         226|
    | 2021-01-01|  2021-01-01|         112|         255|
    | 2021-01-01|  2021-01-01|         255|         232|
    | 2021-01-01|  2021-01-01|         232|         198|
    | 2021-01-01|  2021-01-01|         113|          48|
    | 2021-01-01|  2021-01-01|         239|          75|
    | 2021-01-01|  2021-01-01|         181|         237|
    | 2021-01-01|  2021-01-01|         236|          68|
    | 2021-01-01|  2021-01-01|         256|         148|
    | 2021-01-01|  2021-01-01|          79|          80|
    | 2021-01-01|  2021-01-01|          17|         217|
    | 2021-01-01|  2021-01-01|          62|          29|
    | 2021-01-01|  2021-01-01|          22|          22|
    +-----------+------------+------------+------------+
    only showing top 20 rows
    
    
