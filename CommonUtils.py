import configparser
from typing import Dict, Any
from datetime import datetime


def print_log(logtype, message, functionality="", custom_tag="", statistics_dict=None):
    """
        It's a customized logger function which is used to print the log messages on to the console
    :param logtype: type of the log
    :param message: log message
    :param statistics_dict: if message contains statistics  then pass  each  statistic as key along with its value  in the  statistics_dict  argument
    :return: None
    """
    if statistics_dict is None:
        statistics_dict = {}
    now = datetime.now()
    message_dict = {
        "Functionality": functionality,
        "type": logtype,
        "message": message,
        "custom_tag": custom_tag,
        "timestamp": now.strftime("%d/%m/%Y %H:%M:%S")
    }
    if statistics_dict:
        message_dict["statistics "] = statistics_dict
    print(message_dict)


def get_init_file_object(config_file_path):
    """
    This method takes config file path as an input and returns sections of the config file as a dictionary
    :param config_file_path: ini file extension path
    :return:
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_file_path)
        # converting the config parser object into dict
        my_config_parser_dict = {s: dict(config.items(s)) for s in config.sections()}
        return my_config_parser_dict
    except Exception as ex:
        print(f"-------------------------- Unable to Parse the config file : {config_file_path} due to below error")
        ##raise ex


def check_get_key(tmp_dict: Dict, key: str, recursive=False) -> Any:
    """
    It's used to retrieve the  value of a  key from a python's  dict  object
    :param tmp_dict:  dict python object
    :param key:  if the key is at below 0th depth in the dict object then use the "." to separate the keys '
                                else directly pass the key name
    :param recursive:   if the key is at below 0th depth in the dict object then set it to True
    :return:
    """
    if not "dict" in str(type(tmp_dict)) or len(tmp_dict) == 0:
        print(f"{tmp_dict} is not dictionary or its a empty directory")
        exit(0)
    if recursive:
        key_split = key.split(".")
        for index, key in enumerate(key_split):
            # print(key)
            tmp_dict = tmp_dict.get(key)
            # print(tmp_dict)
            if not tmp_dict:
                print(f"Unable to find key:{key} in dict : {tmp_dict}")
                exit(0)
        return tmp_dict
    else:
        # print("Into else")
        value = tmp_dict.get(key)
        if not value:
            print(f"Unable to find key:{key} in dict : {tmp_dict}")
            exit(0)
        return value
