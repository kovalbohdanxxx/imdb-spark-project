from pyspark.sql import SparkSession # type: ignore

from business_questions import *
from data_loader import *
from utils import *

spark = SparkSession.builder.appName("IMDbDataAnalysis").getOrCreate()

# Get all movies that are available in the Ukrainian language.
ukrainian_movies = get_movies_available_in_ukrainian(load_title_akas(spark))
ukrainian_movies.show(10, truncate=False)

# Get the average rating for each movie genre.
average_rating_by_genre = get_average_rating_by_genre(load_title_basics(spark), load_title_ratings(spark))
average_rating_by_genre.show()

# Get movies with more than 100,000 votes that belong to the "Action" genre.
top_action_movies = get_top_action_movies(load_title_basics(spark), load_title_ratings(spark))
top_action_movies.show(10, truncate=False)

# Get a list of movies where the average rating is greater than 7 and the number of votes exceeds 5000.
top_rated_movies= get_top_rated_movies(load_title_basics(spark), load_title_ratings(spark))
top_rated_movies.show(10, truncate=False)

# Get movies with a rating above 8
high_rated_movies = get_high_rated_movies(load_title_basics(spark), load_title_ratings(spark))
high_rated_movies.show(10, truncate=False)

# Get most voted movies for each year
most_voted_movies_for_each_year = get_most_voted_movie_by_year(load_title_basics(spark), load_title_ratings(spark))
most_voted_movies_for_each_year.show(100)

# sudo docker build -t imdb-spark-img .
# sudo docker run -v /home/kovalbohdanxxx/usr/university/subjects/BigData/data:/data imdb-spark-img
# sudo docker run -v <path_to_folder_with_dataset>:/data imdb-spark-img
