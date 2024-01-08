import traceback

import pandas as pd
from sqlalchemy import create_engine

from CommonUtils import get_init_file_object, check_get_key


def get_config_object(config_path):
    config = get_init_file_object(config_path)
    return config


def get_mysql_connection(database_config, databasename=None):
    host = check_get_key(database_config, "host", False)
    port = check_get_key(database_config, "port", False)
    username = check_get_key(database_config, "username", False)
    password = check_get_key(database_config, "password", False)
    if databasename:
        url = f'mysql+pymysql://{username}:{password}@{host}:{port}/{databasename}'
    else:
        url = f'mysql+pymysql://{username}:{password}@{host}:{port}'
    sqlEngine = create_engine(url)
    dbConnection = sqlEngine.connect()
    return dbConnection


# def create_table_from_dataframe(dbconnection, df, database, tableName, datasource, __functionality__):
#     try:
#         create_database(dbconnection, database)
#         frame = df.to_sql(tableName, dbconnection, schema=database, if_exists='fail')
#     except ValueError as vx:
#         print(vx)
#     except Exception as ex:
#         print_log("TABLE_CREATION_ERROR", f"Unable to create table due to : {traceback.format_exc()}",
#                   functionality=__functionality__, custom_tag=datasource.upper())
#     else:
#         print_log("DEBUG5", f" Table {tableName} created successfully", functionality=__functionality__,
#                   custom_tag=datasource.upper())
def create_table_from_dataframe(dbconnection, df, database, tableName, datasource, __functionality__):
    create_database(dbconnection, database)
    frame = df.to_sql(tableName, dbconnection, schema=database, if_exists='fail')


def create_database(dbconnection, database):
    dbconnection.execute(f'CREATE  DATABASE IF NOT EXISTS {database}')


def drop_table(dbconnect, tablename):
    dbconnect.execute(f'DROP TABLE IF EXISTS {tablename}')


def execute_select_query(dbconnection, query):
    """
        This function is used to execute the select query and returns the query output as a pandas dataframe
    :param dbconnection:
    :param query: select query to be executed
    :return: pandas Dataframe
    """

    return pd.read_sql(query, dbconnection)


def write_to_database(database_config, databasename, tableName, df):
    """
           This function is used to execute the select query and returns the query output as a pandas dataframe
       :param dbconnection:
       :param dbconnection: database name
       :param tableName: tableName
       :param df: dataframe
       :return: None
       """
    dbconnection = None
    try:
        dbconnection = get_mysql_connection(database_config, databasename)
        frame = df.to_sql(f"{tableName}", dbconnection, index=False, if_exists='append')
    except Exception as ex:
        print(traceback.format_exc())
        # raise ex
    finally:
        dbconnection.close()
