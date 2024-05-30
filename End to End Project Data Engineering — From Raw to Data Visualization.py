#!/usr/bin/env python
# coding: utf-8

# In[1]:


# In this project, I will design a comprehensive data workflow that spans Data Extraction, Data Transformation, and Data Visualization. The primary goal is to take raw data and transform it into a usable format for end users, 
#cultimately leading to the creation of insightful visualizations.


# In[2]:


# Objective
# Data Extraction: Collect raw data from databases, APIs, files, or web scraping. Ensure data is relevant and comprehensive.
# Data Transformation: Transform the data from raw into data warehouse (such as BigQuery) and create a consistent and reliable dataset for analysis.
# Data Visualization: Create insightful visualizations from the prepared data.


# In[3]:


# Dataset
# The dataset i am using is about flight ticket searches during the period from March to May 2019.
# This dataset contains information on the search terms used and the results that come out from those search terms.


# In[4]:


# Project Diagram
# We will create data pipelines that can automate and facilitate the effective
# and efficient movement of data from raw to insights.


# ![1.png](attachment:1.png)

# In[6]:


# Step Explanation
# Because here we will create a data pipeline using airflow, we need to import several libraries that we will use within our pipeline frameworks,
# such as MySQLToGCSOperator, GCSToBigQueryOperator, and BashOperator.
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.bash_operator import BashOperator


# In[ ]:


# Next, we need to define default parameters that will be used for all tasks within the DAG. These parameters typically include information about the creator, 
#error handling approaches, and how the DAG will operate.
default_args = {
   "owner": "naufaldy",
   "depends_on_past": False,
   "email_on_failure": False,
   "email_on_retry": False,
   "retries": 1,
   "retry_delay": timedelta(minutes=5),
}


# In[ ]:


# Nest, we need to prepare a blueprint DAG that will serve to execute and automate all the processes 
# we’ve included in the pipelines.
dag = DAG(
    "data_engineering_project_naufaldy", # name of dag
    default_args=default_args,
    description="End to End data engineer project",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
)


# In[ ]:


# Data Source Extraction
# This pipeline will begin with data extraction. Data extraction is the process of retrieving necessary data from various sources
# for analysis purposes.
# In this project, the data will be extracted from MySQL and then loaded into Google Cloud Storage using Airflow.
# MySQL: The database where the flight ticket search data is located.
# Google Cloud Storage: Location where the data we extract will be input and stored here.
# First, we need to prepare a query, which will help us retrieve the necessary information from the data in MySQL. And to execute this query, we’ll need 
# the assistance of the MySQLToGCSOperator.
# Query to extract the data from mysql    
SQL_QUERY = """
select  
 *
from db_flight.flight_tickets ft
"""

# Task for Extract to GCS
extract_mysql = MySQLToGCSOperator(
    task_id="extract_mysql_to_gcs",
    sql=SQL_QUERY,
    bucket="etl-workshop-bucket",
    filename="flight_tickets.json",
    mysql_conn_id="mysql_default",
    gcp_conn_id="google_cloud_gcs",
    dag=dag,
)

# Task workflow
extract_mysql


# In[ ]:


# Transformation Layer
# After the data from MySQL has been stored in Google Cloud Storage, next we will create a Data Warehouse. 
# There are 4 important layers in the creation of a data warehouse, which are:
# Layer 1 — Data Source Layer:
# This layer contains defined data source which will be used to extract analytical information 
# from and load them into our data warehouse.
# In this project context, we will extract data from GCS and load them into our data warehouse
# in BigQuery. To do this, we need help from GCSToBigQueryOperator.
# Task for load to BigQuery (L1 layer)
l1_layers = GCSToBigQueryOperator(
    task_id="load_to_bq",
    bucket="etl-workshop-bucket",
    source_objects=["flight_tickets.json"],
    destination_project_dataset_table="etl_workshop_naufaldy.flight_tickets_raw",
    schema_fields=[
        {"name": "flightId", "type": "STRING", "mode": "REQUIRED"},
        {"name": "searchTerms", "type": "STRING", "mode": "NULLABLE"},
        {"name": "rank", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "snippet", "type": "STRING", "mode": "NULLABLE"},
        {"name": "displayLink", "type": "STRING", "mode": "NULLABLE"},
        {"name": "link", "type": "STRING", "mode": "NULLABLE"},
        {"name": "queryTime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "totalResults", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "cacheId", "type": "STRING", "mode": "NULLABLE"},
        {"name": "formattedUrl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "htmlFormattedUrl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "htmlSnippet", "type": "STRING", "mode": "NULLABLE"},
        {"name": "htmlTitle", "type": "STRING", "mode": "NULLABLE"},
        {"name": "kind", "type": "STRING", "mode": "NULLABLE"},
        {"name": "pagemap", "type": "STRING", "mode": "NULLABLE"},
        {"name": "cseName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "startIndex", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "inputEncoding", "type": "STRING", "mode": "NULLABLE"},
        {"name": "outputEncoding", "type": "STRING", "mode": "NULLABLE"},
        {"name": "safe", "type": "STRING", "mode": "NULLABLE"},
        {"name": "cx", "type": "STRING", "mode": "NULLABLE"},
        {"name": "gl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "searchTime", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "formattedSearchTime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "formattedTotalResults", "type": "STRING", "mode": "NULLABLE"},
    ],
    write_disposition="WRITE_TRUNCATE",
    source_format="NEWLINE_DELIMITED_JSON",
    dag=dag,
)


# In[ ]:


# Layer 2 — Staging Area:
# This part will be the intermediate layer between data sources and Layer 3. Not much is done here,
# usually just process of changing columns names or PII masking.

# From Layer 2–4 we will be utilized DBT and Airflow. First, we need to create a layer 2 dbt model
# that contains the transformation query from the data in layer 1. 
# The DBT model will be executed by a DAG with help of a BashOperator .
# Layer 2 DBT Model
{{ config(materialized='table') }}

with source_data as (
    select
        flightId flight_id,
        searchTerms search_terms,
        rank,  
        title,  
        snippet,  
        displayLink display_link,  
        link,  
        queryTime query_time,  
        totalResults total_results,  
        cacheId cache_id,  
        formattedUrl formatted_url,  
        htmlFormattedUrl html_formatted_url,  
        htmlSnippet html_snippet,  
        htmlTitle html_title,  
        kind,  
        pagemap,  
        cseName cse_name,  
        count,  
        startIndex start_index,  
        inputEncoding input_encoding,  
        outputEncoding output_encoding,  
        safe,  
        cx,  
        gl,  
        searchTime search_time, 
        formattedSearchTime formatted_search_time,  
        formattedTotalResults formatted_total_results
    from `partnatech.etl_workshop_naufaldy.flight_tickets_raw`
),

enrich_data AS (
    select
        *,
        DATE(query_time) as query_date
    from source_data
)

select * from enrich_data


# In[ ]:


# Task who will run the layer 2 DBT Model
l2_transform = BashOperator(
    task_id='l2_transform_naufaldy',
    bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run -s l2_transform_naufaldy',
    dag=dag,
)
# We will do this DBT model and BashOperator until Layer 4.


# In[ ]:


# Layer 3 — Dimentional Layer:
# More complex transformations and data modeling to prepare the data for analysis. Usually, here is where 
# the Fact Tables and Dimensional Tables are formed.
# Layer 3 DBT Model
{{ 
    config(
        materialized='incremental',
        unique_key='flight_id',
        partition_by={
            "field": "query_date",
            "data_type": "date",
            "granularity": "day"
        }
    ) 
}}

SELECT * FROM {{ ref('l2_transform_naufaldy') }}


# In[ ]:


# Task who will run DBT Model
l3_transform = BashOperator(
    task_id='l3_transform_naufaldy',
    bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run -s l3_transform_naufaldy',
    dag=dag,
)


# In[ ]:


# Layer 4 — Presentation Layer:
The final layer where the data is ready for use in Business Intelligence tools and analytics.
# Layer 4 DBT Model
{{ config(
        materialized='table',
        schema='naufaldy'
    ) 
}}

-- select columns that will used from l3 layer
WITH data_source as (
    SELECT flight_id,
        display_link,
        rank,
        search_terms,
        query_date
    FROM {{ ref('l3_transform_naufaldy') }}
),

--transform data 
transform_data as (
  SELECT 
    *,
    REGEXP_EXTRACT(search_terms, r'to\s+(.*)') as destination,
    EXTRACT(MONTH from query_date) as search_month,
    DATE(query_date) as search_date
  FROM data_source
),

-- aggregate data
aggregate_data as (
  SELECT
    search_date,
    search_month,
    destination,
    display_link,
    rank,
    COUNT(display_link) web_company_url_count,
  FROM transform_data
  GROUP BY 1, 2, 3, 4, 5
)

-- query data
SELECT * FROM aggregate_data


# In[ ]:


# Task who will run layer 4 DBT Model
l4_transform = BashOperator(
   task_id='l4_transform_naufaldy',
   bash_command='cd /home/tsabitghazian/airflow_demo/ws2dbt/dbt_partnatech && dbt run -s l4_transform_naufaldy',
   dag=dag,
)


# In[ ]:


# The last thing we need to create is the task workflow, which will organize and manage the execution of each task that we have created 
#within the DAG.
# Task workflow
extract_mysql >> l1_layers >> l2_transform >> l3_transform >> l4_transform


# In[ ]:


# In the context of data engineering and workflow management, a task workflow refers to the sequence and organization of tasks within a Directed Acyclic Graph (DAG). This workflow is crucial for orchestrating tasks in a way that respects their dependencies and execution order, ensuring that the entire pipeline runs smoothly and efficiently. The task workflow is typically managed by workflow automation tools like Apache Airflow, which can schedule and monitor the tasks within 
# a DAG.


# In[ ]:


# Data Visualization
# After the data warehouse layer 4 is formed, now its time to create a dashboars using Looker Studio. This dashboard
# will show or visualize the data that has been formed in layer 4. The goal is that this dashboard can make it easier 
# for business users/stakeholders to obtain information and can
# help in making decisions based on the existing data (data driven decision making.)


# ![2.png](attachment:2.png)

# In[ ]:


# Like the simple dashboard that i have made, with the data in layer 4, stakeholders who used the dashboard can see various 
# information such as Total Search, Total Seach by Month, Web Company that often appears when performing a search, etc.
# If you want to see the dashboard I made, I will share the link here: Data Visualization — Looker Studio

