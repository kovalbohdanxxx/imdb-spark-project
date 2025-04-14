from pyspark.sql.functions import col, array_contains # type: ignore

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

def get_top_action_movies(title_basics_df, title_ratings_df):
	"""
	Get movies with more than 100,000 votes that belong to the "Action" genre.
	"""
	# Join title basics with ratings data on 'tconst'
	movies_with_ratings_df = title_basics_df.join(
		title_ratings_df,
		title_basics_df["tconst"] == title_ratings_df["tconst"],
		how="inner"
	)

	# Filter for movies with more than 100,000 votes and genre including "Action"
	top_action_movies_df = movies_with_ratings_df.filter(
		(col("numVotes") > 100000) &
		array_contains(col("genres"), "Action")
	)

	# Select relevant columns: movie title, genre, and number of votes
	result_df = top_action_movies_df.select(
		"primaryTitle", "genres", "numVotes"
	).orderBy(col("numVotes").desc())

	return result_df

def get_movie_count_by_director():
	# Get the count of movies for each director.
	pass

def get_most_popular_actors():
	# Get the most popular actors based on the number of movies they have been in (with more than 10 movies).
	pass

def get_top_rated_movies(title_basics_df, title_ratings_df):
	"""
	Get a list of movies where the average rating is greater than 7
	and the number of votes exceeds 5000.
	"""
	# Join movie information with ratings data on 'tconst'
	movies_with_ratings_df = title_basics_df.join(
		title_ratings_df,
		title_basics_df["tconst"] == title_ratings_df["tconst"],
		how="inner"
	)

	# Filter for movies with an average rating greater than 7 and more than 5000 votes
	top_rated_movies_df = movies_with_ratings_df.filter(
		(col("titleType") == "movie") &
		(col("averageRating") > 7) &
		(col("numVotes") > 5000)
	)

	# Select relevant columns to display
	result_df = top_rated_movies_df.select(
		"primaryTitle", "startYear", "averageRating", "numVotes"
	)

	return result_df

def get_high_rated_movies(title_basics_df, title_ratings_df):
	"""
	Join movie data and ratings data to find movies with a rating above 8.
	"""
	# Join movie information with ratings data on 'tconst'
	movies_with_ratings_df = title_basics_df.join(
		title_ratings_df,
		title_basics_df["tconst"] == title_ratings_df["tconst"],
		how="inner"
	)

	# Filter for movies with an average rating greater than 8
	high_rated_movies_df = movies_with_ratings_df.filter(
		(col("titleType") == "movie") &
		(col("averageRating") > 8)
	)

	# Select relevant columns to display
	result_df = high_rated_movies_df.select(
		"primaryTitle", "startYear", "averageRating"
	)

	return result_df

def get_movie_count_by_genre_in_year():
	# Count the number of movies released in or after 2010, grouped by genre.
	pass

def get_most_voted_movie_by_year():
	# Find the movie with the most votes for each year using window functions.
	pass