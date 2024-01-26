# 5-1-Project-Data-Pipelines
1. [Project Overview](#schema1)
2. [Prerequisites](#schema2)
3. [Project Instructions](#schema3)
4. [Install, run Airflow](#schema4)
5. [Project Details](#schema5)


[REF](#schemaref)

<hr>
<a name='schema1'></a>


## 1. Project Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to 
create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running 
checks on the data as the final step.


![dag](./img/dag.png)


<hr>
<a name='schema2'></a>


## 2. Prerequisites

**Prerequisites**
- Create an IAM User in AWS.
  - Follow the steps on the page Create an IAM User in AWS in the lesson Data Pipelines.
- Configure Redshift Serverless in AWS.
  - Follow the steps on the page Configure Redshift Serverless in the lesson Airflow and AWS. 
**Setting up Connections**
- Connect Airflow and AWS
  - Follow the steps on the page Connections - AWS Credentials in the lesson Airflow and AWS.
  - Use docker check [4-Automate-Data-Pipelines](../4-Automate-Data-Pipelines/README.md)
- Connect Airflow to AWS Redshift Serverless
  - Follow the steps on the page Add Airflow Connections to AWS Redshift in the lesson Airflow and AWS.

<hr>
<a name='schema3'></a>

## 3. Project Instructions


**Datasets**

For this project, you'll be working with two datasets. Here are the s3 links for each:

Log data: `s3://udacity-dend/log_data`

Song data: `s3://udacity-dend/song-data`

Tip: You will want to copy the data to your own bucket.


<hr>
<a name='schema4'></a>

## 4. Install, run Airflow

Follow the steps of: [5-Data-Quality](../5-1-Project-Data-Pipelines/README.md)


<hr>
<a name='schema5'></a>

## 5. Project Details

- Configuring the DAG. In the DAG, add `default parameters` according to these guidelines
  - The DAG does not have dependencies on past runs
  - On failure, the task are retried 3 times
  - Retries happen every 5 minutes
  - Catchup is turned off
  - Do not email on retry


- Building the operators ( Stage Operator, Fact and Dimension Operators, Data Quality Operator)
To complete the project, you need to build four different operators to stage the data, transform the data, 
and run checks on data quality.

All of the operators and task instances will run SQL statements against the Redshift database. However, 
using parameters wisely will allow you to build flexible, reusable, and configurable operators you can later apply 
to many kinds of data pipelines with Redshift and with other databases.





<hr>
<a name='schemaref'></a>

## REF:
[Sample song_data](https://knowledge.udacity.com/questions/1002142)

```
aws s3 cp s3://udacity-dend/song_data/A/B/C/TRABCAS128F14A25E2.json ~/song-data/ 
```

