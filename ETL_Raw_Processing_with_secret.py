"""
Author : Dillip Kumar Nayak
"""

__functionality__ = "ETL_RAW"

import json
import sys
import traceback

from AWSUtils import get_aws_session, create_aws_client, get_secret, write_to_s3_update_catalog, \
    create_s3_bucket_if_not_exist
from CommonUtils import print_log, check_get_key
from DatabaseUtils import get_mysql_connection, execute_select_query, get_config_object, \
    write_to_database, create_database
from custom_transformations import common_func_execute_with_callable
import boto3

cmd_line_arguments = sys.argv
config_file_path = cmd_line_arguments[1]


def extract(dbconnection, databasename, datasource):
    select_query = f"select * from {databasename}.{datasource}"
    # print(select_query)
    return execute_select_query(dbconnection, select_query)


def transform(df):
    try:
        # df = common_func_execute_without_callable(df)
        df = common_func_execute_with_callable(df)
        """
            # You can some more transformations here
        """
        return df
    except Exception as ex:
        print(f"Unable to apply transformations due to below error")
        raise ex


def load(destination_datasource_aws_config, df, boto3_session, region):
    s3_client = create_aws_client(boto3_session, region, aws_service_name="s3")
    create_s3_bucket_if_not_exist(s3_client, region, destination_datasource_aws_config['destination_s3_bucket_name'])
    s3_dataset_prefix = f"s3://{destination_datasource_aws_config['destination_s3_bucket_name']}/{destination_datasource_aws_config['destination_s3_prefix_name']}"
    partition_cols = str(destination_datasource_aws_config["s3_glue_catalog_partitions_columns"]).split(",")
    write_to_s3_update_catalog(df, boto3_session, s3_dataset_prefix,
                               destination_datasource_aws_config["glue_catalog_database_name"],
                               destination_datasource_aws_config["glue_catalog_table_name"], partition_cols)


def main():
    mysql_con = None
    try:
        print_log("MAIN_INIT", "Started the RAW layer execution", functionality=__functionality__)
        # Step1
        print_log("DEBUG1", f"Read the config file :{config_file_path} and creating config object",
                  functionality=__functionality__)
        configs = get_config_object(config_file_path)
        print_log("DEBUG2", f"Parsing aws security configurations", functionality=__functionality__)
        aws_security_config = check_get_key(configs, "aws_security", True)
        aws_secrets_manager_config = check_get_key(configs, "aws_secret_manager", True)
        aws_session = get_aws_session("access_key_secret", aws_security_config)

        secretsmanager_client = create_aws_client(aws_session, aws_security_config["region"],
                                                  aws_service_name="secretsmanager")
        print_log("DEBUG3", f"Successfully created AWS Session, Secrets Manager Clinet ",
                  functionality=__functionality__)
        print_log("DEBUG4", f"Retrieving  database info from the secrets manager", functionality=__functionality__)
        database_config = json.loads(
            get_secret(aws_secrets_manager_config["myinfra_rds_mysql_username_password"], secretsmanager_client))
        mysql_con = get_mysql_connection(database_config)
        print_log("DEBUG5", f"Successfully acquired the database connection", functionality=__functionality__)
        mysql_config = check_get_key(configs, "mysql")
        datapackage_databasename = check_get_key(mysql_config, "datapackage_database")
        datasources_to_create = str(check_get_key(mysql_config, "datasources")).split(",")
        # Step1
        for datasource in datasources_to_create:
            try:
                print_log("EXTRACT", f"Extracting data from {datapackage_databasename}.{datasource}",
                          custom_tag=datasource.upper(), functionality=__functionality__)
                # Step3
                datasource = datasource.strip()
                extract_df = extract(mysql_con, datapackage_databasename, datasource)
                # Step4
                print_log("TRANSFORM",
                          f"Applying transformations on  {datapackage_databasename}.{datasource} dataframe",
                          custom_tag=datasource.upper(), functionality=__functionality__)
                transformed_df = transform(extract_df)
                # adding datasource column
                transformed_df["datasource"] = datasource
                # Step5
                datasource_config = check_get_key(configs, datasource, True)
                s3_dataset_prefix = f"s3://{datasource_config['destination_s3_bucket_name']}/{datasource_config['destination_s3_prefix_name']}"
                print_log("LOAD",
                          f"Loading transformed data into   s3:{s3_dataset_prefix}, glue_database:{datasource_config['glue_catalog_database_name']}glue_table:{datasource_config['glue_catalog_table_name']}",
                          custom_tag=datasource.upper(), functionality=__functionality__)
                load(datasource_config, transformed_df, aws_session, aws_security_config["region"])
                print_log("LOAD_SUCCESS",
                          f"Successfully Loaded transformed data into   s3:{s3_dataset_prefix}, glue_database:{datasource_config['glue_catalog_database_name']}glue_table:{datasource_config['glue_catalog_table_name']}",
                          custom_tag=datasource.upper(), functionality=__functionality__)
            except Exception as ex:
                print_log("DATASOURCE_ERROR",
                          f"Unable to process datasource successfully due to {traceback.format_exc()} ",
                          custom_tag=datasource.upper(), functionality=__functionality__)
    except Exception as ex:
        print_log("MAIN_ERROR", f"Error in processing main program due to :  {traceback.format_exc()} ",
                  functionality=__functionality__)
    finally:
        mysql_con.close()
        print_log("MAIN_END", f"Completed ", functionality=__functionality__)


if __name__ == '__main__':
    main()
