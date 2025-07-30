1. I did use a backfill to download the data
2. I also used ForEach with Subflow

### Quiz Questions

Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1. Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

#### Answer
I used `du -h filename` after the file download and unzip to find the file size. However, I could also comment the purge section of the flow and run `docker exec -it kestra-postgresql-pgadmin-kestra-1 /bin/bash` to find the file and preview its size inside the kestra container. The third way would be the Outputs tab in Kestra.

- **128.3 MiB**
- 134.5 MiB
- 364.7 MiB
- 692.6 MiB

2. What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv`
- **`green_tripdata_2020-04.csv`**
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

3. How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
```sql
SELECT COUNT(*) FROM yellow_tripdata
WHERE filename LIKE '%2020%'
```

- 13,537.299
- **24,648,499**
- 18,324,219
- 29,430,127

4. How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
```sql
SELECT COUNT(*) FROM green_tripdata
WHERE filename LIKE '%2020%'
```

- 5,327,301
- 936,199
- **1,734,051**
- 1,342,034

5. How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
```sql
SELECT COUNT(*) FROM yellow_tripdata
WHERE filename LIKE '%2021-03%'
```

- 1,428,092
- 706,911
- **1,925,152**
- 2,561,031

6. How would you configure the timezone to New York in a Schedule trigger?
This information is in [Kestra's docs](https://kestra.io/docs/workflow-components/triggers/schedule-trigger.)

- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration
- **Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration