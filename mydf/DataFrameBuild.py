import numpy as np
import pandas as pd
import pyspark as spark

# print(dir(spark))

# ---------------------------------------------------------
# df_array(x):

# ---------------------------------------------------------

class myDF():
    """
    :param simple_array:
    :param randomly_increasing_array:
    """
    def __init__(self,spark):
        # super(myDF, self).__init__()
        # self.arg = arg
        # self.df =
        self.spark = spark

    # def print_df(self):
        # self.count = self.df.count()

    def simple_array(self,*args):
        '''
        Create a simple, linear array.
        '''
        if len(args) == 0:
            n = 10
            s = 0
            total = n + 1
            self.df = self.spark.range(n).toDF("Index")

        elif len(args) == 1:
            if args[0] < 1:
                sys.exit(1)
            n = args[0]
            s = 0
            total = n + 1
            self.df = self.spark.range(n).toDF("Index")
        else:
            n = args[1]
            s = args[0]
            total = n - s + 1
            x = np.linspace(s,n,total)
            pdx = pd.DataFrame(x,columns=["Index"])
            self.df = self.spark.createDataFrame(pdx)

        self.count = total

        return self.df

    def randomly_increasing_array(self,n):
        """
        Randomly increasing variance.
        Build a 2D DataFrame from numpy arrays.
        arr1: linspaced
        arr2: random (increasing from between 1 and current row)
        """
        # print("Hello!!!!-2")
        domain = np.linspace(0,n-1,n)
        # print(domain)

        num_random = [np.random.randint(0,x+1) for x in domain]
        print(num_random)

        # 2 tall columns
        data = np.vstack([domain,num_random]).transpose()

        # print(data.shape)

        # to pandas
        pdx = pd.DataFrame(data,columns=["Index","Random Number"])

        # to spark
        dfx = self.spark.createDataFrame(pdx)

        self.df = dfx
        return self.df
