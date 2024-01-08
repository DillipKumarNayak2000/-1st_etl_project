"""
Author : Dillip Kumar Nayak
"""

__functionality__ = "ETL_UNIFIED"

import json
import sys
import traceback

from pandas import DataFrame
import pandasql as psql

from AWSUtils import get_aws_session, get_secret, create_aws_client, read_from_athena, create_s3_bucket_if_not_exist, \
    write_to_s3_update_catalog
from CommonUtils import print_log, check_get_key
from DatabaseUtils import get_mysql_connection, execute_select_query, get_config_object, \
    write_to_database, create_database

cmd_line_arguments = sys.argv
config_file_path = cmd_line_arguments[1]


def extract(boto3_session, datasource_config) -> DataFrame:
    select_query = f"select * from {datasource_config['glue_catalog_database_name']}.{datasource_config['glue_catalog_table_name']};"
    print(select_query)
    print(datasource_config["glue_catalog_database_name"])
    return read_from_athena(boto3_session, select_query, datasource_config["glue_catalog_database_name"])


def transform(df):
    return df


def load(destination_datasource_aws_config, df, boto3_session, region):
    s3_client = create_aws_client(boto3_session, region, aws_service_name="s3")
    create_s3_bucket_if_not_exist(s3_client, region, destination_datasource_aws_config['destination_s3_bucket_name'])
    s3_dataset_prefix = f"s3://{destination_datasource_aws_config['destination_s3_bucket_name']}/{destination_datasource_aws_config['destination_s3_prefix_name']}"
    partition_cols = []
    if destination_datasource_aws_config["s3_glue_catalog_partitions_columns"] != "no":
        partition_cols = str(destination_datasource_aws_config["s3_glue_catalog_partitions_columns"]).split(",")

    write_to_s3_update_catalog(df, boto3_session, s3_dataset_prefix,
                               destination_datasource_aws_config["glue_catalog_database_name"],
                               destination_datasource_aws_config["glue_catalog_table_name"], partition_cols)


def main():
    mysql_con = None
    raw_datasource_dataframes = []
    try:
        print_log("MAIN_INIT", "Started the RAW layer execution", functionality=__functionality__)
        print_log("DEBUG1", f"Read the config file :{config_file_path} and creating config object",
                  functionality=__functionality__)
        print_log("DEBUG2", f"Parsing aws security configurations", functionality=__functionality__)
        configs = get_config_object(config_file_path)
        aws_security_config = check_get_key(configs, "aws_security", True)
        aws_session = get_aws_session("access_key_secret", aws_security_config)
        database_config = check_get_key(configs, "mysql", True)
        datasources_to_create = str(check_get_key(database_config, "datasources")).split(",")
        print_log("DEBUG3", f"Extracting required tables and creating respective dataframes ",
                  functionality=__functionality__)
        for datasource in datasources_to_create:
            datasource_config = check_get_key(configs, datasource, True)
            print(datasource_config)
            tmp_df = extract(aws_session, datasource_config)
            print(tmp_df)
            raw_datasource_dataframes.append(tmp_df)
            print_log("EXTRACT", f"Extracted the table :{datasource}", custom_tag=datasource.upper(),
                      functionality=__functionality__)
        gdp, cpi, inflation, population, cash_surplus_deficit = raw_datasource_dataframes
        for unified_data_source in str(check_get_key(database_config, "unified_data_sources")).split(","):
            unified_data_source_config = check_get_key(configs, unified_data_source, True)
            with open(unified_data_source_config["glue_catalog_table_name"], 'r') as file:
                select_query = file.read()
                print_log("CUSTOM_QUERY", f"Executing query : {select_query} ",
                          custom_tag=unified_data_source_config["glue_catalog_table_name"],
                          functionality=__functionality__)
                df = psql.sqldf(select_query)
                print_log("TRANSFORM", f"Applying Transformations for : {select_query} ",
                          custom_tag=unified_data_source_config["glue_catalog_table_name"],
                          functionality=__functionality__)
                transformed_df = transform(df)
                row, col = transformed_df.shape
                if row > 0:
                    s3_dataset_prefix = f"s3://{unified_data_source_config['destination_s3_bucket_name']}/{unified_data_source_config['destination_s3_prefix_name']}"
                    print_log("LOAD", f"Loading transformed data into table : {unified_data_source_config['glue_catalog_table_name']} ",
                              custom_tag=unified_data_source_config["glue_catalog_table_name"],
                              functionality=__functionality__)
                    load(unified_data_source_config, transformed_df, aws_session, aws_security_config["region"])
                    print_log("LOAD_SUCCESS",
                              f"Successfully Loaded transformed data into   s3:{s3_dataset_prefix}, glue_database:{unified_data_source_config['glue_catalog_database_name']}glue_table:{unified_data_source_config['glue_catalog_table_name']}",
                              custom_tag=unified_data_source_config["glue_catalog_table_name"],
                              functionality=__functionality__)
    except Exception as ex:
        print_log("MAIN_ERROR", f"Error in processing main program due to :  {traceback.format_exc()} ",
                  functionality=__functionality__)
    finally:
        print_log("MAIN_END", f"Completed ", functionality=__functionality__)


if __name__ == '__main__':
    main()
