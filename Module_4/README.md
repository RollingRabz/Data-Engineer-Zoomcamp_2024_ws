# Module 4: Analytics Engineering 
Goal: Transforming the data loaded in DWH into Analytical Views

### Prerequisites

- A running warehouse (BigQuery will be use in this module) 
- A set of running pipelines ingesting the project dataset (module 3 completed)
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
  
  <img src="ae.PNG" />
  
* ETL vs ELT
  
  - ETL
    
      - Slightly more stable and compliant data analysis
  
      - Higher storage and computing costs

  - ELT
  
      - Faster and more flexible data analysis
  
      - Lower cost and lower maintenance


* Data modeling concepts (fact and dim tables)
  
  - Facts tables
    
      - Measurements, metrics or facts
    
      - Corresponds to a business process
    
      - “verbs” ex. sales, orders
   
   - Dimensions tables
     
      - Corresponds to a business identity
        
      - Provides context to a business process
        
      - “nouns” ex. customer, product


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
