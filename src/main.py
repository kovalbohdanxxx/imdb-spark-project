from pyspark.sql import SparkSession # type: ignore

from business_questions import *
from data_loader import *
from utils import *

spark = SparkSession.builder.appName("IMDbDataAnalysis").getOrCreate()

# Get all movies that are available in the Ukrainian language.
ukrainian_movies = get_movies_available_in_ukrainian(load_title_akas(spark))
ukrainian_movies.show(10, truncate=False)


# sudo docker run -v <path_to_folder_with_dataset>:/data imdb-spark-img
# sudo docker run imdb-spark-img