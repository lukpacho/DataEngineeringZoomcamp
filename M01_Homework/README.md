## Question 1. Understanding docker first run

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

### Answer
I used a command
```bash
docker run --rm -it python:3.12.8 bash
```
The pip version was 24.3.1


## Question 2. Understanding Docker networking and docker-compose
Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

### Answer
When using docker composer it creates a network so you should use container ports, and the name of postgres host, i.e. `db:5432`.


## Prepare Postgres
I used my files from week1/docker_sql, where I:
    1) built Dockerfile taxi_ingest for ingesting tripdata and zones
    2) modified docker-compose.yaml to start postgresql, ingest data and run pgadmin
    3) ran docker-compose.yaml and connected to pgAdmin


## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:

1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles

### Answer
I got an answer of 
104830
198995
109642
27686
35201.
Probably due the different number of rows in the file as my file got 476_386. Below is the query.

```sql
SELECT 
	CASE
		WHEN trip_distance <= 1 					    THEN '(0, 1] mile(s)'
		WHEN trip_distance >  1 AND trip_distance <= 3  THEN '(1, 3] mile(s)'
		WHEN trip_distance >  3 AND trip_distance <= 7  THEN '(3, 7] mile(s)'
		WHEN trip_distance >  7 AND trip_distance <= 10 THEN '(7, 10] mile(s)'
		ELSE '(10, +oo) mile(s)'
	END 		AS distance_bucket,
	COUNT(*) 	AS trips
FROM 
	green_trips
WHERE
		lpep_pickup_datetime >= '2019-10-01'
	AND
		lpep_pickup_datetime < '2019-11-01'
GROUP BY
	distance_bucket
ORDER BY
	MIN(
		CASE
		WHEN trip_distance <= 1 					    THEN 1
		WHEN trip_distance >  1 AND trip_distance <= 3  THEN 2
		WHEN trip_distance >  3 AND trip_distance <= 7  THEN 3
		WHEN trip_distance >  7 AND trip_distance <= 10 THEN 4
		ELSE 5
		END
	)
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

### Answer
31 of Oct 2019
```sql
SELECT 
	MAX(trip_distance) as max_distance,
	DATE_TRUNC('DAY', lpep_pickup_datetime) as "day"
FROM 
	green_trips
GROUP BY
	"day"
ORDER BY
	max_distance DESC
```

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in `total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.

### Answer
- East Harlem North, East Harlem South, Morningside Heights

```sql
SELECT 
	SUM(t.total_amount) AS sum_amount,
	DATE_TRUNC('DAY', t.lpep_pickup_datetime) AS "day",
	z."Zone"
FROM 
	green_trips AS t JOIN zones z
	ON t."PULocationID" = z."LocationID"
WHERE
	DATE_TRUNC('DAY', t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY
	z."Zone", DATE_TRUNC('DAY', t.lpep_pickup_datetime)
ORDER BY
	sum_amount DESC
```

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

### Answer.
The answer is JFK Airport, maximum tip was 87.3

```sql
SELECT 
	MAX(tip_amount) AS max_tip,
	zdo."Zone" as dropoff_zone
FROM 
	green_trips AS t 
	JOIN zones zpu
		ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo
		ON t."DOLocationID" = zdo."LocationID"
WHERE
		lpep_pickup_datetime >= '2019-10-01'
	AND
		lpep_pickup_datetime < '2019-11-01'
	AND
		t."PULocationID" = (
			SELECT "LocationID" FROM zones
			WHERE "Zone" = 'East Harlem North'
		)
GROUP BY
	zdo."Zone"
ORDER BY
	max_tip DESC
```


## Terraform
In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

I've used terraform for the `main.tf` and `main.tf` with `variables.tf` for the GCP service.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for:

1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform

### Answer
```bash
terraform init
terraform apply -auto-approve
terraform destroy
```