#### Usecase:

Attached file is Product Inventory Snapshot(Inventory_snapshot_data.cpgz) at warehouse level. Below are the problems to be solved.

Load the file to BigQuery

Creating Staging layer table/tables with Normalized data(No Arrays)
 
#### Ask:

Python script to preprocess and load the file to BQ

DBT codes for Staging

#### How to use:

Create a service account and give it biquery admin privilages in IAM of GCP.

Create a key and rename it as "bigquery_key.json"

Change your project name in bigquery-cleaning-challenge\saras_analytics\.dbt\profiles.yml

I'm adding a video to show how to work with the code below (Short loom video)

https://youtu.be/2H1FMEC4W7Q
