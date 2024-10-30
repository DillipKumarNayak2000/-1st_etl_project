# -1st_etl_project
#config.ini file contains all the related information of the prject , which is besically contains all the configuration of the project on the config.ini we need to give our mysql connection , aws access key,aws screat maneger id,secreat access key and all the information of the s3 buckets all those things it contains we need to give before project execution.


requirement.txt contains all the requirement modules files we need to import inside aws linux instance before project execution.


1_DataPreparation_rolesbased.py and 1_DataPreparation_with_secret.py is the 1 step of the project execution it will execute 1st thats why the name starts with 1_.


ETL_Raw_Processing_rolebased.py and ETL_Raw_Processing_with_scret.py is the 2nd step of the project execution after the data store in the s3 it will execute basically it will process the etl raw data and perform cleaning activity.


ETL_Unified_Processing_with_secret.py and ETL_Unified_Processing_rolebased.py file is the 3rd or last layer of the project which will perform action on the unified data on the top of aws using glue and athena.
# ETL Unified Processing Project

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for processing data using AWS services. The pipeline consists of multiple steps, each responsible for a specific part of the data processing workflow, leveraging AWS S3, Glue, and Athena.

## Project Structure

The project is organized into several Python scripts, each serving a distinct purpose:

- **1_DataPreparation_rolesbased.py**: Initial data preparation using role-based access.
- **1_DataPreparation_with_secret.py**: Initial data preparation using AWS Secrets Manager for credentials.
- **ETL_Raw_Processing_rolebased.py**: Processes raw data stored in S3, performing necessary cleaning activities.
- **ETL_Raw_Processing_with_secret.py**: Similar to the previous script, but retrieves credentials from AWS Secrets Manager.
- **ETL_Unified_Processing_rolebased.py**: Performs actions on the unified data using AWS Glue and Athena.
- **ETL_Unified_Processing_with_secret.py**: Similar to the previous script, using Secrets Manager for secure access.
- **AWSUtils.py**: Contains utility functions for AWS interactions.
- **CommonUtils.py**: General utility functions used across various scripts.
- **DatabaseUtils.py**: Functions related to database operations, specifically MySQL.
- **DatapackageUtils.py**: Utilities for handling data packages.
- **test.py**: Test cases for validating the functionality of the ETL pipeline.
- **README.md**: Project documentation.
- **config.ini**: Configuration file containing database connections, AWS credentials, and S3 bucket information.
- **requirement.txt**: List of required Python modules for the project.

## Configuration

The `config.ini` file contains all necessary configurations for the project. It should include:

- MySQL connection details
- AWS access key and secret
- AWS Secrets Manager ID
- S3 bucket information

Make sure to fill out this file before executing the project.

## Requirements

Before running the project, install the required Python packages listed in the `requirement.txt` file. You can do this using:

```bash
% pip install -r requirement.txt

## Execution Steps:

Data Preparation: Run either 1_DataPreparation_rolesbased.py or 1_DataPreparation_with_secret.py to prepare the data for further processing.

Raw Processing: Execute ETL_Raw_Processing_rolebased.py or ETL_Raw_Processing_with_secret.py to process and clean the raw data stored in S3.

Unified Processing: Finally, run ETL_Unified_Processing_rolebased.py or ETL_Unified_Processing_with_secret.py to perform operations on the unified dataset using AWS Glue and Athena.


## Notes
Ensure that you have the necessary permissions set in AWS to access S3, Glue, and Athena services.
This project is designed to run in an AWS Linux instance, so ensure your environment is correctly configured.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author
Dillip Kumar Nayak
