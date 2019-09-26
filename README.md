# AWS Consumption KPI Metrics Extract
This is to extract AWS KPIs specifically for consumption and pricing and dump them into MySql database model for further reporting.
This module is set up using Python version 2.7 but can be modified for other versions as well. An already existing api SDK library for python `boto3` is being used to communicate with AWS


# Steps:

1. Install python. 

2. Install AWSCLI:
  `Pip install awscli`

3. verify the installation path of awscli:
  `where aws`

4. Configure AWSCLI for root user:
  `aws configure`
  note- [this will create two files ../.aws/credentials  &  ../.aws/config]

5. Configure AWSCLI for IAM user (substitute your IAM profile name):
  `aws configure --profile <IAM_profile_name>`

6. Install boto:
  `pip install -U boto`

7. Install boto3:
  `pip install -U boto3`

8. get_Bucket_properties.py -- This should be run first as this will load the lookup table with S3 bucket's versioning and Regions into csv files

9. get_public_access.py --This should be run next - It fetches the Public access lookup data into csv files

10. cloudwatch_get_metrics_statistics.py -- This should be run next to get Billing and S3 Cloudwatch metrics into csv files

