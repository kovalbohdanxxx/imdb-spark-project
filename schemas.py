from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType # type: ignore

def get_name_basics_schema():
	name_basics_schema = StructType([
		StructField("nconst", StringType(), True),            # unique identifier for the name/person
		StructField("primaryName", StringType(), True),       # most often credited name of the person
		StructField("birthYear", IntegerType(), True),        # birth year of the person (YYYY)
		StructField("deathYear", StringType(), True),         # death year of the person (YYYY), '\N' if not applicable
		StructField("primaryProfession", StringType(), True), # top-3 professions of the person
		StructField("knownForTitles", StringType(), True)     # titles the person is known for
	])
	return name_basics_schema

def get_title_akas_schema():
	title_akas_schema = StructType([
		StructField("titleId", StringType(), True),         # unique identifier for the title
		StructField("ordering", IntegerType(), True),       # uniquely identifies rows for a given titleId
		StructField("title", StringType(), True),           # localized title
		StructField("region", StringType(), True),          # region for this version of the title
		StructField("language", StringType(), True),        # language of the title
		StructField("types", StringType(), True),           # types like alternative, dvd, festival, etc.
		StructField("attributes", StringType(), True),      # additional terms to describe the title
		StructField("isOriginalTitle", IntegerType(), True) # 0: not original title; 1: original title
	])
	return title_akas_schema

def get_title_basics_schema():
	title_basics_schema = StructType([
	StructField("tconst", StringType(), True),             # unique identifier of the title
		StructField("titleType", StringType(), True),      # type/format of the title (e.g., movie, short, etc.)
		StructField("primaryTitle", StringType(), True),   # more popular title or title used in promotion
		StructField("originalTitle", StringType(), True),  # original title in the original language
		StructField("isAdult", BooleanType(), True),       # 0: non-adult title; 1: adult title
		StructField("startYear", StringType(), True),      # release year or start year for TV Series
		StructField("endYear", StringType(), True),        # end year for TV Series, '\N' for other titles
		StructField("runtimeMinutes", StringType(), True), # runtime of the title in minutes
		StructField("genres", StringType(), True)          # genres associated with the title (up to 3)
	])
	return title_basics_schema

def get_title_crew_schema():
	title_crew_schema = StructType([
		StructField("tconst", StringType(), True),    # unique identifier for the title
		StructField("directors", StringType(), True), # array of directors for the title
		StructField("writers", StringType(), True)    # array of writers for the title
	])
	return title_crew_schema

def get_title_episode_schema():
	title_episode_schema = StructType([
		StructField("tconst", StringType(), True),        # unique identifier for the episode
		StructField("parentTconst", StringType(), True),  # unique identifier for the parent TV series
		StructField("seasonNumber", IntegerType(), True), # season number the episode belongs to
		StructField("episodeNumber", IntegerType(), True) # episode number within the TV series
	])
	return title_episode_schema

def get_title_principals_schema():
	title_principals_schema = StructType([
		StructField("tconst", StringType(), True),    # unique identifier for the title
		StructField("ordering", IntegerType(), True), # number to uniquely identify rows for a given titleId
		StructField("nconst", StringType(), True),    # unique identifier of the name/person
		StructField("category", StringType(), True),  # category of the person's job (e.g., actor, producer)
		StructField("job", StringType(), True),       # specific job title if applicable, else '\N'
		StructField("characters", StringType(), True) # name of the character played if applicable, else '\N'
	])
	return title_principals_schema

def get_title_ratings_schema():
	title_ratings_schema = StructType([
		StructField("tconst", StringType(), True),        # unique identifier for the title
		StructField("averageRating", DoubleType(), True), # weighted average of all user ratings
		StructField("numVotes", IntegerType(), True)      # number of votes the title has received
	])
	return title_ratings_schema
