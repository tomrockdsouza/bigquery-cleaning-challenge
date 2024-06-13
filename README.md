####Usecase:
Attached file is Product Inventory Snapshot(Inventory_snapshot_data.cpgz) at warehouse level. Below are the problems to be solved.
Load the file to BigQuery
Creating Staging layer table/tables with Normalized data(No Arrays)
 
####Ask:
Python script to preprocess and load the file to BQ
DBT codes for Staging


Create a service account and give it biquery admin privilages in IAM of GCP.
Create a key and rename it as "bigquery_key.json"
Change your project name in bigquery-cleaning-challenge\saras_analytics\.dbt\profiles.yml


I'm adding a video to show how to work with the code below (Short loom video)
https://www.loom.com/share/8ab189e43942412592c550e39b454068?sid=2d2c2dcb-9cdc-4f09-a443-d1f511a5adcd