## Week 3 Homework

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>

CREATE OR REPLACE EXTERNAL TABLE ny_taxi.green_trips_external
(VendorID            integer
, lpep_pickup_datetime   timestamp
, lpep_dropoff_datetime  timestamp
, store_and_fwd_flag     string  
, RatecodeID           float64       
, PULocationID         integer        
, DOLocationID         integer       
, passenger_count        float64       
, trip_distance          float64       
, fare_amount            float64       
, extra                  float64       
, mta_tax                float64       
, tip_amount             float64       
, tolls_amount           float64       
, ehail_fee              string     
, improvement_surcharge  float64       
, total_amount           float64       
, payment_type           float64       
, trip_type              float64       
, congestion_surcharge   float64
) OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-tdowning/2022_green_taxi_parquets_from_tlc/*']
);

Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>

CREATE OR REPLACE TABLE ny_taxi.green_trips
AS
SELECT * FROM ny_taxi.green_trips_external;


## Question 1:

Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402  <-- ANSWER
- 1,936,423
- 253,647

select count(1) from ny_taxi.green_trips;

## Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 

select count(distinct PULocationID) from ny_taxi.green_trips_external;
-- estimate of 0MB read

select count(distinct PULocationID) from ny_taxi.green_trips; 
-- estimate of 6.41MB read

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
- 0 MB for the External Table and 6.41MB for the Materialized Table <-- ANSWER
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622  <-- ANSWER

select count(1) from ny_taxi.green_trips where fare_amount = 0;

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID <-- ANSWER
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

CREATE OR REPLACE TABLE ny_taxi.green_trips_partitioned
PARTITION BY date(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS
(select * from ny_taxi.green_trips);


## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table <- ANSWER
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table


select distinct PULocationID from ny_taxi.green_trips
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
-- 12.82 MB

select distinct PULocationID from ny_taxi.green_trips_partitioned
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
-- 1.12 MB

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket <-- ANSWER
- Big Table
- Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False <- ANSWER.  

Depends on size of table, how often you're writing to the table, etc.


## (Bonus: Not worth points) Question 8:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

select count(*) from ny_taxi.green_trips_partitioned;

ANSWER:  estimate is 0 bytes.  Why?  I have to assume it's a limitation of the Estimator since the  latest article from Google BigQuery says 
that count(*) reads all of the data in the table in order to get the number.
https://cloud.google.com/knowledge/kb/bigquery-count-query-slot-utilization-and-rows-scanned-000004360



  
