import numpy as np
import pandas as pd
import pyspark as spark


def getDataFrame_simple_array(n,columns=1):
    '''
    Return a simple array based dataframe of n columns
    :param n = max integer (# of rows, 1 .. n)
    :param columns = number of columns
    '''
    return spark.range(n).toDF("number")


def getDataFrame_random_increase(rows):
    """
    Randomly increasing variance.
    Build a 2D DataFrame from numpy arrays.
    arr1: linspaced
    arr2: random (increasing from between 1 and current row)
    """
    r = np.linspace(1,rows,rows)
    num_random = [np.random.randint(0,r[i]) for i in range(rows)]
    # print(num_random)

    # 2 tall columns
    data = np.vstack([r,num_random]).transpose()

    # to pandas
    pdx = pd.DataFrame(data,columns=["Row","Random"])

    # to spark
    dfx = spark.createDataFrame(pdx)

    return dfx
