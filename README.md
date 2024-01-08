# -1st_etl_project
config.ini file contains all the related information of the prject , which is besically contains all the configuration of the project on the config.ini we need to give our mysql connection , aws access key,aws screat maneger id,secreat access key and all the information of the s3 buckets all those things it contains we need to give before project execution.
requirement.txt contains all the requirement modules files we need to import inside aws linux instance before project execution.
1_DataPreparation_rolesbased.py and 1_DataPreparation_with_secret.py is the 1 step of the project execution it will execute 1st thats why the name starts with 1_.
ETL_Raw_Processing_rolebased.py and ETL_Raw_Processing_with_scret.py is the 2nd step of the project execution after the data store in the s3 it will execute.
