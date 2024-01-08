"""
Author : Prudhvi Akella
"""
import pandas as pd
from datetime import date


def add_today_datetime_col_to_df(df):
    df['datetime'] = pd.to_datetime('today')
    return df


def add_today_date_col_to_df(df):
    df['partition_date'] = pd.to_datetime('today').date()
    return df


def add_day_col_to_df(df, column="datetime"):
    df['partition_day'] = df[column].dt.day
    return df


def add_month_col_to_df(df, column="datetime"):
    df['partition_month'] = df[column].dt.month
    return df


def add_year_col_to_df(df, column="datetime"):
    df['partition_year'] = df[column].dt.year
    return df


def common_func_pipeline_list():
    return [add_today_datetime_col_to_df, add_today_date_col_to_df, add_day_col_to_df, add_month_col_to_df,
            add_year_col_to_df]


def common_func_execute_without_callable(df):
    df = add_today_datetime_col_to_df(df)
    df = add_today_date_col_to_df(df)
    df = add_day_col_to_df(df)
    df = add_month_col_to_df(df)
    df = add_year_col_to_df(df)
    return df


def common_func_execute_with_callable(df):
    for func in common_func_pipeline_list():
        if not callable(func):
            raise TypeError("Error Expected callable")
        df = func(df)
    return df
