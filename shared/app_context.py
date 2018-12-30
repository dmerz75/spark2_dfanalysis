from pyspark.sql import SparkSession


class ApplicationContext(object):
    def __init__(self,job_name):
        '''
        A spark application context.
        '''
        self.job_name = job_name
        self.spark = self._create_spark_session(job_name)
        self.sc = self.spark.sparkContext

    @staticmethod
    def _create_spark_session(job_name):
        return SparkSession.builder.appName(job_name).getOrCreate()
