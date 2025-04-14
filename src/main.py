from business_questions import *
from spark_session import *
from data_loader import *
from utils import *
from cfg import *

def main():
	spark = get_spark_session()

	title_akas_df = load_title_akas(spark)
	title_basics_df = load_title_basics(spark)
	title_ratings_df = load_title_ratings(spark)
	title_episode_df = load_title_episode(spark)

	display_dataframe_info(title_akas_df)
	display_dataframe_info(title_basics_df)
	display_dataframe_info(title_ratings_df)
	display_dataframe_info(title_episode_df)


	print(f"{GREEN}Getting all movies that are available in the Ukrainian language.{RESET}")
	ukrainian_movies = get_movies_available_in_ukrainian(title_akas_df)
	ukrainian_movies.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting the average rating for each movie genre.{RESET}")
	average_rating_by_genre = get_average_rating_by_genre(title_basics_df, title_ratings_df)
	average_rating_by_genre.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting movies with more than 100,000 votes that belong to the 'Action' genre.{RESET}")
	top_action_movies = get_top_action_movies(title_basics_df, title_ratings_df)
	top_action_movies.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting movies with an average rating greater than 7 and more than 5000 votes.{RESET}")
	top_rated_movies = get_top_rated_movies(title_basics_df, title_ratings_df)
	top_rated_movies.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting movies with a rating above 8.{RESET}")
	high_rated_movies = get_high_rated_movies(title_basics_df, title_ratings_df)
	high_rated_movies.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting movie count by genre after 2010.{RESET}")
	movie_count_by_genre_after_2010 = get_movie_count_by_genre_in_year(title_basics_df)
	movie_count_by_genre_after_2010.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting most voted movies for each year.{RESET}")
	most_voted_movies_for_each_year = get_most_voted_movie_by_year(title_basics_df, title_ratings_df)
	most_voted_movies_for_each_year.show(SHOW_LIMIT, truncate=False)


	print(f"{GREEN}Getting TV series with the most episodes.{RESET}")
	top_tv_series_by_episodes = get_top_tv_series_by_episodes(title_basics_df, title_episode_df)
	top_tv_series_by_episodes.show(truncate=False)

if __name__ == "__main__":
	main()

# sudo docker build -t imdb-spark-img .
# sudo docker run -v /home/kovalbohdanxxx/usr/university/subjects/BigData/data:/data imdb-spark-img
# sudo docker run -v <path_to_folder_with_dataset>:/data imdb-spark-img
