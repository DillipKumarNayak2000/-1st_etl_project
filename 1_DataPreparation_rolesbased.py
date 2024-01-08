"""
Preparing Data
"""
import json

from CommonUtils import check_get_key, print_log
from DatabaseUtils import get_mysql_connection, drop_table, create_table_from_dataframe, get_config_object
from DatapackageUtils import get_datapackage_resource_df, datapackage_endpoint_table_dict

from AWSUtils import get_aws_session, create_aws_client, get_secret
import sys
import traceback

cmd_line_arguments = sys.argv
config_file_path = cmd_line_arguments[1]
__functionality__ = "ETL_PREPARE_DATA"


def prepare_data(datapackage_endpoint_table_dict, datasource, mysql):
    try:
        df = None
        url = check_get_key(datapackage_endpoint_table_dict, f"{datasource}.endpoint", True)
        if url:
            print_log("DEBUG2", f"Making API call to datapackage to get the data  using {url}", functionality=__functionality__, custom_tag=datasource.upper())
            df = get_datapackage_resource_df(url)
            print_log("DEBUG2", f"Sample data", functionality=__functionality__, custom_tag=datasource.upper())
            # get the first 10 rows
            print(df.head(10), "mediawiki")
            # # shape return tuple of row count and column count
            row_count, col_count = df.shape
            if row_count > 0:
                print_log("DEBUG3", f"Record count ", functionality=__functionality__, custom_tag=datasource.upper(),
                          statistics_dict={"row_count": row_count})
                database_name = check_get_key(datapackage_endpoint_table_dict, f"{datasource}.database", True)
                table_name = check_get_key(datapackage_endpoint_table_dict, f"{datasource}.tablename", True)
                full_table_name = f"{database_name}.{table_name}"
                print_log("DEBUG4", f"Drop the table : {full_table_name} if exist", functionality=__functionality__, custom_tag=datasource.upper())
                drop_table(mysql, full_table_name)
                print_log("DEBUG5", f" Creating table  : {full_table_name} and dummping the data", functionality=__functionality__, custom_tag=datasource.upper())
                create_table_from_dataframe(mysql, df, database_name, table_name, datasource,__functionality__)
            else:
                print_log("ROW_COUNT_ERROR", f"No records to process", functionality=__functionality__, custom_tag=datasource.upper(), statistics_dict={"row_count": 0})
    except Exception as ex:
        print_log("ERROR", f"Error occurred in prepare_data function due to {traceback.format_exc(),}",
                  functionality=__functionality__, custom_tag=datasource.upper())


def main():
    try:
        print_log("MAIN_INIT", "Trying to create mysql database connection",functionality=__functionality__)
        print_log("DEBUG1", f"Read the config file :{config_file_path} and creating config object",functionality=__functionality__)
        # Step1: Parsing config file and creating a dict object
        configs = get_config_object(config_file_path)
        # Step2: Creating database connection
        # database_config = check_get_key(configs, "mysql", True)
        aws_security_config = check_get_key(configs, "aws_security", True)
        aws_secrets_manager_config = check_get_key(configs, "aws_secret_manager", True)
        aws_session = get_aws_session("role",aws_security_config)
        secretsmanager_client = create_aws_client(aws_session, aws_security_config["region"], aws_service_name="secretsmanager")
        database_config = json.loads(get_secret(aws_secrets_manager_config["myinfra_rds_mysql_username_password"], secretsmanager_client))
        mysql_con = get_mysql_connection(database_config)
        datasources_to_create = ["gdp", "cpi", "inflation", "population", "cash_surplus_deficit"]
        for datasource in datasources_to_create:
            print_log("DEBUG1", f"Preparing the data",functionality=__functionality__,custom_tag=datasource.upper())
            prepare_data(datapackage_endpoint_table_dict, datasource, mysql_con)
        print_log("MAIN_CLOSE", "Main successfully executed",functionality=__functionality__)
    except Exception as ex:
        print_log("MAIN_ERROR", f"Unable to execute the main function successfully due to : {traceback.format_exc()}",functionality=__functionality__)


if __name__ == "__main__":
    main()
