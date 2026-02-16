# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](../../../04-analytics-engineering/setup/)
2. Load the Green and Yellow taxi data for 2019-2020 into your warehouse
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

## Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:
```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies) <--
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

**Answer:** `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned`

When you run `dbt run --select int_trips_unioned`, dbt automatically builds all upstream dependencies (models that `int_trips_unioned` depends on) plus the selected model itself.

---

## Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:
```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code <--
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

**Answer:** dbt will fail the test, returning a non-zero exit code

The `accepted_values` test will detect the new value `6` which is not in the allowed list `[1, 2, 3, 4, 5]` and fail the test, returning a non-zero exit code. This is critical for CI/CD pipelines as it stops deployment when data quality issues are detected.

---

## Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- 12,184 <--
- 15,421
```sql
SELECT COUNT(*) as total_records
FROM `practica-cloud-485603.zoomcamp.fct_monthly_zone_revenue`;
```

**Answer:** 12,184

The `fct_monthly_zone_revenue` table contains monthly aggregated revenue data by zone for the 2019-2020 period (24 months × ~263 active zones).

---

## Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- East Harlem North <--
- Morningside Heights
- East Harlem South
- Washington Heights South
```sql
SELECT 
    dz.zone,
    SUM(fmzr.revenue_monthly_total_amount) as total_revenue_2020
FROM `practica-cloud-485603.zoomcamp.fct_monthly_zone_revenue` fmzr
LEFT JOIN `practica-cloud-485603.zoomcamp.dim_zones` dz 
    ON fmzr.zone_id = dz.location_id
WHERE EXTRACT(YEAR FROM fmzr.revenue_month) = 2020
  AND fmzr.service_type = 'green'
GROUP BY dz.zone
ORDER BY total_revenue_2020 DESC
LIMIT 1;
```

**Answer:** East Harlem North ($1,424,104.45)

---

## Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- 384,624 <--
- 421,509
```sql
SELECT 
    SUM(total_monthly_trips) as total_trips_oct_2019
FROM `practica-cloud-485603.zoomcamp.fct_monthly_zone_revenue`
WHERE revenue_month = '2019-10-01'
  AND service_type = 'green';
```

**Answer:** 384,624 (387,006 actual result, closest option)

---

## Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- 43,244,693 <--
- 22,998,722
- 44,112,187

**stg_fhv_tripdata.sql:**
```sql
select
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pulocationid as int64) as pickup_location_id,
    cast(dolocationid as int64) as dropoff_location_id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(sr_flag as int64) as sr_flag

from {{ source('raw_data', 'fhv_tripdata') }}
where dispatching_base_num is not null
```
```sql
SELECT COUNT(*) 
FROM `practica-cloud-485603.zoomcamp.stg_fhv_tripdata`;
```

**Answer:** 43,244,693

---
