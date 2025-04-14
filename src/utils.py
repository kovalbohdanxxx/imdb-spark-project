from cfg import YELLOW, RESET, SHOW_LIMIT, DISPLAY_DATAFRAME_INFO_LINE_LENGTH

def display_dataframe_info(df):
	def print_section(title):
		print(f"{YELLOW}{'=' * DISPLAY_DATAFRAME_INFO_LINE_LENGTH}")
		print(title)
		print(f"{'=' * DISPLAY_DATAFRAME_INFO_LINE_LENGTH}{RESET}")

	print_section("DataFrame Overview")
	print(f"{YELLOW}Number of rows: {df.count():,}")
	print(f"Number of columns: {len(df.columns)}{RESET}")

	print_section("First 10 Rows")
	df.show(SHOW_LIMIT, truncate=False)

	print_section("Schema")
	df.printSchema()

	print_section("Summary Statistics")
	df.describe().show(truncate=False)

	print_section("Unique Values Count")
	for column in df.columns:
		unique_count = df.select(column).distinct().count()
		print(f"{column:<20}: {unique_count:,} unique values")
	print(f"{YELLOW}{'=' * DISPLAY_DATAFRAME_INFO_LINE_LENGTH}{RESET}")
