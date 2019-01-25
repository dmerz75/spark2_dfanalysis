<!-- [(home)](https://dmerz75.github.io/spark2_dfanalysis) -->

# Beginning a Jupyter notebook for PySpark!

[Initial Configuration](Initial_Configuration.md)
    - Python libraries, pandas, numpy, and pyspark

```python
# Spark!
from pyspark import SparkContext
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("myapp").getOrCreate()

#     spark = SparkSession.builder.master("yarn")\
#     .config("spark.executor.instances", "32")\
#     .config("spark.executor.cores", "4")\
#     .config("spark.executor.memory", "4G")\
#     .config("spark.driver.memory", "4G")\
#     .config("spark.executor.memoryOverhead","4G")\
#     .config("spark.yarn.queue","Medium")\
#     .appName("myapp")\
#     .getOrCreate()

sc = spark.sparkContext
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
spark.conf.set("spark.debug.maxToStringFields","true")
```

[Initial Configuration + DataFrameBuild](Standard_Configs.md)
    - My paths and libraries

```python
from shared.app_context import *
from builder.DataFrameBuild import *

ctx = ApplicationContext("Dev-Job")
print(ctx.spark)

DFB = DataFrameBuild(ctx.spark)
```
