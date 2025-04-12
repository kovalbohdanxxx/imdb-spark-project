import os

from pyspark.sql.types import BooleanType # type: ignore
from pyspark.sql.functions import split # type: ignore

from schemas import *

def load_name_basic(spark, path_to_file='/data/name.basics.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	name_basics_df = spark.read.csv(path_to_file, header=True, schema=get_name_basics_schema(), sep="\t")
	name_basics_df = name_basics_df.replace('\\N', None)
	name_basics_df = name_basics_df.withColumn("primaryProfession", split(name_basics_df["primaryProfession"], ",").cast("array<string>"))
	name_basics_df = name_basics_df.withColumn("knownForTitles", split(name_basics_df["knownForTitles"], ",").cast("array<string>"))
	return name_basics_df

def load_title_akas(spark, path_to_file='/data/title.akas.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	title_akas_df = spark.read.csv(path_to_file, header=True, schema=get_title_akas_schema(), sep="\t")
	title_akas_df = title_akas_df.replace('\\N', None)
	title_akas_df = title_akas_df.withColumn("types", split(title_akas_df["types"], ",").cast("array<string>"))
	title_akas_df = title_akas_df.withColumn("attributes", split(title_akas_df["attributes"], ",").cast("array<string>"))
	title_akas_df = title_akas_df.withColumn("isOriginalTitle", title_akas_df["isOriginalTitle"].cast(BooleanType()))
	return title_akas_df

def load_title_basics(spark, path_to_file='/data/title.basics.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	title_basics_df = spark.read.csv(path_to_file, header=True, schema=get_title_basics_schema(), sep="\t")
	title_basics_df = title_basics_df.replace('\\N', None)
	title_basics_df = title_basics_df.withColumn("genres", split(title_basics_df["genres"], ",").cast("array<string>"))
	title_basics_df = title_basics_df.withColumn("isAdult", title_basics_df["isAdult"].cast(BooleanType()))
	return title_basics_df

def load_title_crew(spark, path_to_file='/data/title.crew.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	title_crew_df = spark.read.csv(path_to_file, header=True, schema=get_title_crew_schema(), sep="\t")
	title_crew_df = title_crew_df.replace('\\N', None)
	title_crew_df = title_crew_df.withColumn("directors", split(title_crew_df["directors"], ",").cast("array<string>"))
	title_crew_df = title_crew_df.withColumn("writers", split(title_crew_df["writers"], ",").cast("array<string>"))
	return title_crew_df

def load_title_episode(spark, path_to_file='/data/title.episode.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	title_episode_df = spark.read.csv(path_to_file, header=True, schema=get_title_episode_schema(), sep="\t")
	title_episode_df = title_episode_df.replace('\\N', None)
	return title_episode_df

def load_title_principals(spark, path_to_file='/data/title.principals.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	title_principals_df = spark.read.csv(path_to_file, header=True, schema=get_title_principals_schema(), sep="\t")
	title_principals_df = title_principals_df.replace('\\N', None)
	return title_principals_df

def load_title_ratings(spark, path_to_file='/data/title.ratings.tsv'):
	if not os.path.exists(path_to_file):
		raise FileNotFoundError(f"The file at {path_to_file} was not found.")

	title_ratings_df = spark.read.csv(path_to_file, header=True, schema=get_title_ratings_schema(), sep="\t")
	title_ratings_df = title_ratings_df.replace('\\N', None)
	return title_ratings_df