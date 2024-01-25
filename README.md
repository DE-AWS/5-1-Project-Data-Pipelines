# 5-1-Project-Data-Pipelines
1. [Project Overview](#schema1)
2. [ Prerequisites](#schema2)


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
