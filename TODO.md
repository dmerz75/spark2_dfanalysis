# TODO

## use select, distinct -> column -> get array


# Need to get the process of Notebook to MD to HTML down.


# Things to do for spark2_dfanalysis:
- more dataframe types
- read, write, csv and parquet
- analysis examples.



2 questions:
If I want to add a column that is calculated from other col date, i.e. col('price') - col('discount'),
then I have to use this "lit" command,
df_shop = dfShoppingHabitsWithLapsed.withColumn("cycle_date",lit(config['period_week']))

in this case, is period_week equivalent to cycle_date?
(i know it wasn't for transactions .. but we didn't grab a lot of cycle_dates for the shopping habits)

I wrote it here: it was
path_shop = "gs://e451_pos_acds_transaction_v92j_7b25/shopping_habits/"
It was 328,538,698. so I chose 8 partitions.
df_shop.coalesce(8).write.parquet(mode="overwrite",path=path_shop)


from pyspark.sql import SparkSession
# from .dictmerge import dict_merge
# from ..metadata.metalogger import MetaLogger
# from .filewrapper import FileWrapper


class ApplicationContext(object):
    def __init__(self, job_name):
        # job_name = app_options.get('job_name', 'An_8451_PySpark_job')

        #################################################
        # Spark objects
        #################################################
        self.spark = self._create_spark_session(job_name)
        self.sc = self.spark.sparkContext

    @staticmethod
    def _create_spark_session(job_name):
        return SparkSession.builder.appName(job_name).getOrCreate()
