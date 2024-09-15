# Module_4: Analytics Engineering 
Goal: Transforming the data loaded in DWH into Analytical Views

### Prerequisites
By this stage of the course you should have already: 

- A running warehouse (BigQuery or postgres) 
- A set of running pipelines ingesting the project dataset (week 3 completed)
- The following datasets ingested from the course [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/): 
  * Yellow taxi data - Years 2019 and 2020
  * Green taxi data - Years 2019 and 2020 
  * fhv data - Year 2019. 

## Setting up your environment 
  
- Setting up dbt for using BigQuery (cloud)
- Open a free developer dbt cloud account following [this link](https://www.getdbt.com/signup/)
- Following these instructions to connect to your BigQuery instance[ Set up](dbt_cloud_setup.md)


## Content

### Introduction to analytics engineering

* What is analytics engineering?
* ETL vs ELT 
* Data modeling concepts (fact and dim tables)



### What is dbt? 

* Introduction to dbt 



## Starting a dbt project



### dbt models

* Anatomy of a dbt model: written code vs compiled Sources
* Materialisations: table, view, incremental, ephemeral  
* Seeds, sources and ref  
* Jinja and Macros 
* Packages 
* Variables


### Testing and documenting dbt models
* Tests  
* Documentation 



## Deployment



## Visualising the transformed data
