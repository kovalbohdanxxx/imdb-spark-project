from pyspark.sql import SparkSession  # type: ignore

from cfg import SPARK_APP_NAME

def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
	"""
	Creates and returns a SparkSession with the given application name.
	"""
	return SparkSession.builder.appName(app_name).getOrCreate()
