[(home)](https://dmerz75.github.io/spark2_dfanalysis)

# Basic / Routine / Common operations!

<!-- - [Standard PySpark Configuration](Standard_Configs.html)
- [Initial Configuration for Jupyter notebook for PySpark](Initial_Configuration.md) -->
<!-- - [Building DataFrames](Building_DataFrames1.html) -->

[Building DataFrames](Building_DataFrames1.md)
  - Use DataFrameBuild.build_array to get some sample string, integer, or double columns
```python
mystr = DFB.build_array("string",num=12,width=8)
myint = DFB.build_array("integer",num=12,nrange=(0,4))    # inclusive on range
mydoub = DFB.build_array("double",num=12,nrange=(10,10.1))
```

[Example DataFrames](Example_Dataframes.md)
  - Some sample dataframes to copy.

[Read/Write](Read_Write_Partition.md)

[User Defined Functions for Date, Datetime](udf_date_time.md) join, udf, lambda, date, datetime, withColumn, drop, concat, unique identifier
```python
# UDF's with lambdas!

# functions:
def nohyphens(z):
    return re.sub("-","",str(z))

# udf's:
udf_nohyphen_str = udf(lambda z: nohyphens(z),StringType())
udf_nohyphen_int = udf(lambda z: int(nohyphens(z)),IntegerType())

# remove the hyphens!
df_nohyphen = df_dated2\
.withColumn("int-date",udf_nohyphen_int(col("date-hyphen")))\
```

```python
unique_id = [col('make'),lit('_'),col('vin'), lit('_'), col('year')]

df_id = df_sales.withColumn('key',concat(*unique_id))
```

(coming soon!)

  - Read in dataframe, do a basic comparison.
  - Read/write for loop with some count/logical comparisons.
