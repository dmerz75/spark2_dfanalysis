import pandas as pd
import numpy as np
import pyspark


class DataFrameProfile():
    """
    A class to profile a dataframe
    """
    def __init__(self, df):
        # super(myDF, self).__init__()
        # self.arg = arg
        print("Profile a dataframe")

    def print_size(self,):
        pass


    def profile_size(self):
        pass

    def profile_partitions(self):
        pass



def column_comparison(df1,df2):
    '''
    Get the column names present in both dataframes.
    :param df1: first dataframe
    :param df2: second dataframe
    '''
    df1_cols = df1.columns
    df2_cols = df2.columns
    only1_cols = set(df1_cols) - set(df2_cols)
    only2_cols = set(df2_cols) - set(df1_cols)
    common = list(set(df1_cols).intersection(set(df2_cols)))
    print("Unique to df1:\n\t",only1_cols)
    print("Unique to df2:\n\t",only2_cols)
    print("Members in both:\n\t",common)
    return common
