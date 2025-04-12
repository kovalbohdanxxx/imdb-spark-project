from pyspark.sql.functions import col # type: ignore

def get_movies_available_in_ukrainian(title_akas_df):
	"""
	Get all movies that are available in the Ukrainian language.
	"""
	# Filter the DataFrame to get all movies available in Ukrainian
	ukrainian_movies_df = title_akas_df.filter(col("language") == "uk")

	# Select relevant columns
	ukrainian_movies_df = ukrainian_movies_df.select("titleId", "title", "language", "isOriginalTitle")

	return ukrainian_movies_df

def get_average_rating_by_genre():
	# Get the average rating for each movie genre.
	pass

def get_top_action_movies():
	# Get movies with more than 100,000 votes that belong to the "Action" genre.
	pass

def get_movie_count_by_director():
	# Get the count of movies for each director.
	pass

def get_most_popular_actors():
	# Get the most popular actors based on the number of movies they have been in (with more than 10 movies).
	pass

def get_top_rated_movies():
	# Get a list of movies where the average rating is greater than 7 and the number of votes exceeds 5000.
	pass

def get_high_rated_movies():
	# Join movie data and ratings data to find movies with a rating above 8.
	pass

def get_movie_count_by_genre_in_year():
	# Count the number of movies released in or after 2010, grouped by genre.
	pass

def get_most_voted_movie_by_year():
	# Find the movie with the most votes for each year using window functions.
	pass