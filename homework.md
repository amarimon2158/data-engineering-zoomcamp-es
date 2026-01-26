# Module 1 Homework: Docker & SQL

## Question 1. What's the version of pip in the python:3.13 image?

- 25.3 <-
- 24.3.1
- 24.2.1
- 23.3.1


## Question 2. Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database? 

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432 <-

## Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile? 

```sql

SELECT COUNT(*) 
FROM green_taxi_trips 
WHERE trip_distance <= 1;
```
Output:

```
+----------------------+
|     | count          |          
|----------------------+
|  1  |     8009       |               |
+----------------------+
```

Answer: 8007

## Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles.

```sql

SELECT 
    DATE(lpep_pickup_datetime) as pickup_date,
    MAX(trip_distance) as max_distance
FROM green_taxi_trips
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```

Output:

```
+----------------------+---------------+
|     | pickup_date    | max_distante  |
|----------------------+---------------|
|  1  | 2025-11-14     |    88.03      |
+----------------------+---------------+
```

Answer: 2025-11-14


## Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?


```sql

SELECT 
    tz."Zone",
    SUM(gt.total_amount) as total_sum
FROM green_taxi_trips gt
JOIN taxi_zones tz ON gt."PULocationID" = tz."LocationID"
WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
GROUP BY tz."Zone"
ORDER BY total_sum DESC
LIMIT 10;
```

Output:

```
+---------------------+-------------------+
| Zone                | total_sum         |
|---------------------+-------------------|
| East Harlem North   | 9281.919999999991 |
| East Harlem South   | 6696.130000000004 |
| Central park        | 2378.7899999999995|
+---------------------+-------------------+
```
Answer: East Harlem North

## Question 6. Largest tip


```sql

SELECT 
    dropoff_zone."Zone" as dropoff_zone,
    MAX(gt.tip_amount) as max_tip
FROM green_taxi_trips gt
JOIN taxi_zones pickup_zone ON gt."PULocationID" = pickup_zone."LocationID"
JOIN taxi_zones dropoff_zone ON gt."DOLocationID" = dropoff_zone."LocationID"
WHERE pickup_zone."Zone" = 'East Harlem North'
  AND EXTRACT(YEAR FROM gt.lpep_pickup_datetime) = 2025
  AND EXTRACT(MONTH FROM gt.lpep_pickup_datetime) = 11
GROUP BY dropoff_zone."Zone"
ORDER BY max_tip DESC
LIMIT 10;

```

Output:

```
+----------------+---------+
| dropoff_zone   | max_tip |
|----------------+---------|
| Yorkville West | 81.89   |
+----------------+---------+
```
Answer: Yorkville West

## Question 7. Which of the following sequences describes the Terraform workflow for: 1) Downloading plugins and setting up backend, 2) Generating and executing changes, 3) Removing all resources?

- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy <-
- terraform import, terraform apply -y, terraform rm