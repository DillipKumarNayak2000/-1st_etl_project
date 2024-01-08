import datapackage
import pandas as pd
from tabulate import tabulate

base_data_url = 'https://datahub.io/core/'

datapackage_endpoint_table_dict = {
    "gdp": {
        "endpoint": "gdp/datapackage.json",
        "database": "datapackage",
        "tablename": "gdp",
        "description": ""
    },
    "cpi": {
        "endpoint": "cpi/datapackage.json",
        "database": "datapackage",
        "tablename": "cpi",
        "description": ""
    },
    "inflation": {
        "endpoint": "inflation/datapackage.json",
        "database": "datapackage",
        "tablename": "inflation",
        "description": ""
    },
    "population": {
        "endpoint": "population/datapackage.json",
        "database": "datapackage",
        "tablename": "population",
        "description": ""
    },
    "cash_surplus_deficit": {
        "endpoint": "cash-surplus-deficit/datapackage.json",
        "database": "datapackage",
        "tablename": "cash_surplus_deficit",
        "description": ""
    },
}

def print_df_using_tabulate(df,format):
    """
    :param df:  panda dataframe
    :param format:  “plain”,“simple”,“github”,“grid”,“fancy_grid”,“pipe”,“orgtbl”,“jira”,“presto”,“pretty”,“psql”,“rst”,“mediawiki”,“moinmoin”,“youtrack”,“html”,“latex”,“latex_raw”,“latex_booktabs”,“textile”
    :return: None
    """
    print(tabulate(df, headers='keys', tablefmt=format))


def get_datapackage_resource_df(absolute_file_url):
    """

    :param absolute_file_url:  datapackage file URL
    :return: Pandas DF
    """
    # to load Data Package into storage
    global df
    data_url = f"{base_data_url}{absolute_file_url}"
    package = datapackage.Package(data_url)
    # to load only tabular data
    resources = package.resources
    print(resources)
    for resource in resources:
        if resource.tabular:
            df = pd.read_csv(resource.descriptor['path'])
            #df.info()
            df.columns = df.columns.str.replace(' ', '_')

    return df