# -*- coding: utf-8 -*-
"""
Created on Wed May 10 20:22:50 2023

@author: ggnar
"""

import logging
import os
import subprocess
import yaml
import pandas as pd
import dask.dataframe as dd
import datetime 
import gc
import re


def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)

def validate_dataset(config, new_dataset):
    # Read the new dataset using Dask DataFrame
    df = dd.read_csv(new_dataset)
    df.columns = cleanColumnHeaders(df.columns)

    # Get the expected column names from the configuration
    expected_columns = config['columns']

    # Check if all expected columns are present in the new dataset

    if len(df.columns) == len(expected_columns) and list(expected_columns)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_columns))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_columns).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        return 0



def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def cleanColumnHeaders(columns):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    columns = columns.str.lower()
    columns = columns.str.replace('[^\w]','_',regex=True)
    columns = list(map(lambda x: x.strip('_'), list(columns)))
    columns = list(map(lambda x: replacer(x,'_'), list(columns)))
    columns =list(map(lambda x: x.lower(), list(columns)))
    return columns
   