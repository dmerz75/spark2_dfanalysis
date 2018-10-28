import re
import random
import string

import numpy as np
import pandas as pd

import pyspark as spark
from pyspark.sql.types import *
from pyspark.sql import functions as sf

# ---------------------------------------------------------
# df_array(x):
# ---------------------------------------------------------

class DataFrameBuild():
    """
    :param simple_array:
    :param randomly_increasing_array:
    :param profile_size:
    :param profile_partitions:
    """
    def __init__(self,spark):

        self.spark = spark

    def print_size(self,df):

        print(df.count())

    def build_array(self,atype,**kwargs):
        '''
        PRIMARY ARRAY builder. This function generates arrays of many different
        types including string, integer, and double.
        :param atype: (req.) string,integer,double
        :param btype: simple, gaussian, increasing
        :param num: number of elements
        :param width: length of any one string
        :param nrange: low to high boundaries
        :param low: low end boundary
        :param high: high end boundary
        '''
        num = 10
        width = 8
        nrange = (1,11)
        low = nrange[0]
        high = nrange[1]

        # print('nrange' in kwargs)
        # print(type(kwargs))

        # Check kwargs
        if 'num' in kwargs:
            num = kwargs['num']
        if 'width' in kwargs:
            width = kwargs['width']
        if 'nrange' in kwargs:
            nrange = kwargs['nrange']
            low = nrange[0]
            high = nrange[1]
        if 'low' in kwargs:
            low = nrange[0]
        if 'high' in kwargs:
            high = nrange[1]

        # print(nrange,type(nrange))
        # print(nrange,type(nrange[0]))
        # print(low,high,type(low),type(high))

        if atype == 'string':
            str_arr = [''.join(random.choices(string.ascii_lowercase,k=width))
                       for x in range(num)]
            return str_arr

        if atype == 'integer':
            arr = np.array([random.randint(low,high) for x in range(num)])
            return arr

        if atype == 'double':
            arr = np.array([random.uniform(low,high) for x in range(num)])
            return arr

    def get_StructField(self,ctype):
        '''
        Pass in variable (string,integer,double)
        If not 'str','int','double', get the type.
        '''
        stypes = ['str','int','int32',
                  'double','float64']

        # print(ctype)
        if ctype not in stypes:
            ctype = type(ctype).__name__
            # print(ctype)
            if ctype not in stypes:
                raise Exception("Type was not 'str','int',or 'double'.")

        if re.search('str',ctype):
            field = StructField(ctype,StringType(),True)
            return field,ctype

        if re.search('int',ctype):
            field = StructField(ctype,IntegerType(),True)
            return field,ctype

        if re.search('float',ctype):
            field = StructField(ctype,FloatType(),True)
            return field,ctype

        if re.search('double',ctype):
            field = StructField(ctype,DoubleType(),True)
            return field,ctype

        return field,ctype


    def arrays_to_dataframe(self,lst_arr,lst_names=None):
        '''
        This function converts a list of arrays and a list of names (columns)
        into a dataframe with column headers having the names in the list.
        :param lst_arr: list of arrays
        :param lst_names: (optional) give column names.
        '''
        # print(type(lst_arr))
        # print(type(lst_names))
        #
        # print(lst_names)
        # print(len(lst_arr),len(lst_names))

        if lst_names == None:
            run_ctype = True
        else:
            run_ctype = False

        # if it goes in as a list, -> becomes a list of lists.
        # submitted: ['qvfdlpks', 'vpyviyux', 'ypqyinsl']
        # result: [['qvfdlpks', 'vpyviyux', 'ypqyinsl']]

        # lst_arr: array, string, list, list of lists, list of array/list
        top_level = type(lst_arr).__name__
        next_level = None

        if top_level == 'list':

            next_level = type(lst_arr[0]).__name__

            if ((next_level != 'list') and (next_level != 'ndarray')):
                lst_arr = [lst_arr]
        else:
            lst_arr = [lst_arr]

        # print("Levels:")
        # print(top_level,next_level)
        # print(lst_arr)

        # if type(lst_arr).__name__ == 'list':
        #
        #     # if list, then check if it's
        #     print(type(lst_arr[0]).__name__)
        #     if ((type(lst_arr[0]).__name__ != 'list') and
        #         (type(lst_arr[0]).__name__ != 'ndarray')):
        #         # turn list submitted to list of list.
        #         lst_arr = [lst_arr]

        if (type(lst_names).__name__ != 'list'):
            lst_names = [lst_names]

        # print(type(lst_arr))
        # print(type(lst_names))
        # print(len(lst_arr),len(lst_names))
        # return

        # collect ctypes
        if run_ctype == True:
            fields = [] # StructField
            ctypes = [] # str,int,float,double
            for arr in lst_arr:
                field,ctype = self.get_StructField(arr[0])
                # print(field,type(field))
                fields.append(field)
                ctypes.append(ctype)
            # print(fields)
            # print(ctypes)
            # return
            lst_names = ctypes

        # print("check:")
        # print(lst_arr,len(lst_arr))
        # print(lst_names)
        # return

        # multiple equal size lists, zipped into 1 list of tuples
        if len(lst_arr) == 1:
            lst_zipped = lst_arr[0]
        else:
            lst_zipped = list(zip(*lst_arr))

        # print('zipped:',lst_zipped)

        # Final DataFrame (Pandas)
        pdx = pd.DataFrame(lst_zipped,columns=lst_names)

        # Final DataFrame (Spark)
        self.df = self.spark.createDataFrame(pdx)
        return self.df

        # ?? can I skip pandas?
        dfx = self.spark.createDataFrame(lst_zipped,fields)
        self.df = dfx
        return dfx




# Arrays:
#     def get_string_array(self,w=8):
#         '''
#         Get an array of strings.
#         :param w: the width of the string generated.
#         '''
#         # print("need a string of %d characters" % w)
#         str1 = ''.join(random.choices(string.ascii_lowercase,k=w))
#
#         # print(len(str1),w)
#         return str1
#
#     def get_integer_array(self,nrange):
#         '''
#         Get an array of integers
#         '''
#         # print("need %d integers" % w)
#         num = random.randint(nrange[0],nrange[1])
#         return num
#
#
#     def get_double_array(self,nrange):
#         '''
#         Get an array of doubles
#         '''
#         # print("need %d doubles" % n)
#         # num = random.random() # 0.0 - 1.0
#         num = random.uniform(nrange[0],nrange[1])
#         return num


    # def df_simple_array(self,*args):
    #     ''' REMOVE?
    #     Needs to pass in an array
    #     Create a simple, linear array.
    #     '''
    #     if len(args) == 0:
    #         n = 10
    #         s = 0
    #         total = n + 1
    #         self.df = self.spark.range(n).toDF("Index")
    #
    #     elif len(args) == 1:
    #         if args[0] < 1:
    #             sys.exit(1)
    #         n = args[0]
    #         s = 0
    #         total = n + 1
    #         self.df = self.spark.range(n).toDF("Index")
    #     else:
    #         n = args[1]
    #         s = args[0]
    #         total = n - s + 1
    #         x = np.linspace(s,n,total)
    #         pdx = pd.DataFrame(x,columns=["Index"])
    #         self.df = self.spark.createDataFrame(pdx)
    #
    #     self.count = total
    #     return self.df
    #
    # def df_randomly_increasing_array(self,n):
    #     """REMOVE?
    #     Randomly increasing variance.
    #     Build a 2D DataFrame from numpy arrays.
    #     arr1: linspaced
    #     arr2: random (increasing from between 1 and current row)
    #     """
    #     # print("Hello!!!!-2")
    #     domain = np.linspace(0,n-1,n)
    #     # print(domain)
    #
    #     num_random = [np.random.randint(0,x+1) for x in domain]
    #     # print(num_random)
    #
    #     # 2 tall columns
    #     data = np.vstack([domain,num_random]).transpose()
    #
    #     # print(data.shape)
    #
    #     # to pandas
    #     pdx = pd.DataFrame(data,columns=["Index","Random Number"])
    #
    #     # to spark
    #     dfx = self.spark.createDataFrame(pdx)
    #
    #     self.df = dfx
    #     return self.df
    #
    # def df_string_array(self,lst,label='value'):
    #     '''REMOVE?
    #     :param lst: a list of strings
    #     '''
    #     dfx = self.spark.createDataFrame(lst,StringType()).toDF(label)
    #     self.df = dfx
    #     return self.df
    #
    # def decision_array_type(self,tup_ctype):
    #     '''
    #     Get an array as strings, integers, or doubles.
    #     :param tup_ctype:
    #     ('string1',6)               => tcxivu
    #     ('string2',13)              => rlftefdzayehr
    #     ('integer',(1000,9999))     => 4285
    #     ('double1',(0.0,1.0))       => 0.6514825745809212
    #     ('double2',(0.0,10000000))  => 2142944.0838629534
    #     ('integer2',(1,10000))      => 3081
    #     '''
    #     # print("Getting a single array of type:",ctype," with %d rows." % n)
    #
    #     ctype = tup_ctype[1]
    #     width = tup_ctype[2]
    #
    #     if re.search('str',ctype):
    #         arr = self.get_string_array(width)
    #         # str1 = ''.join(random.choices(string.ascii_lowercase,k=w))
    #         return arr
    #
    #     if re.search('int',ctype):
    #         arr = self.get_integer_array(width)
    #         # num = random.randint(nrange[0],nrange[1])
    #         return arr
    #
    #     if re.search('double',ctype):
    #         nrange = width
    #         arr = self.get_double_array(nrange)
    #         # num = random.uniform(nrange[0],nrange[1])
    #         return arr
    #



    # def df_multidimensional(self,r,c,column_types):
    #     '''
    #     :param r:
    #     :param c:
    #     :param column_types:
    #     '''
    #     lst_total = []
    #     lst_fields = []
    #
    #     for ctype in column_types:
    #
    #         field = self.get_StructField(ctype[1])
    #         lst_arr = []
    #
    #         for i in range(r):
    #
    #             entry = self.decision_array_type(ctype)
    #             lst_arr.append(entry)
    #
    #         lst_fields.append(field)
    #         lst_total.append(lst_arr)
    #
    #     # print(lst_fields)
    #     # print(lst_total)
    #
    #     # multiple equal size lists, zipped into 1 list of tuples
    #     lst_zipped = list(zip(*lst_total))
    #
    #     # print(lst_zipped)
    #     # print("zipped,columns")
    #     # print(lst_zipped)
    #     # print(column_types)
    #
    #     pdx = pd.DataFrame(lst_zipped,columns=[c[0] for c in column_types])
    #     self.df = self.spark.createDataFrame(pdx)
    #     return self.df
    #
    #     # ?? can I skip pandas?
    #     dfx = self.spark.createDataFrame(lst_total,lst_fields).toDF(column_types)
    #     self.df = dfx
    #     return dfx
