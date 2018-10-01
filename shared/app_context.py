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
