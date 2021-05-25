# Description
This project automates monitoring of the data warehouse ETL pipelines via Apache Airflow.
The data pipelines are created to be reusable and backfill easily. Because data quality was important
for the project there are quality checks. The data source is S3 made of JSON logs which is processed 
in a data warehouse hosted on Amazon Redshift.


