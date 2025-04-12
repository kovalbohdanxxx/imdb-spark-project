def display_dataframe_info(df):
	GREEN = "\033[32m"
	RESET = "\033[0m"
	LINE_LENGTH = 55

	print(f"{GREEN}{'='*LINE_LENGTH}")
	print(f"DataFrame Overview")
	print(f"{'='*LINE_LENGTH}{RESET}")
	print(f"{GREEN}Number of rows: {df.count():,}")
	print(f"Number of columns: {len(df.columns)}")
	print(f"{'='*LINE_LENGTH}{RESET}")

	print(f"{GREEN}First 5 rows:")
	df.show(5, truncate=False)
	print(f"{'='*LINE_LENGTH}{RESET}")

	print(f"{GREEN}Schema:")
	df.printSchema()
	print(f"{'='*LINE_LENGTH}{RESET}")

	print(f"{GREEN}Summary statistics for numeric columns:")
	df.describe().show(truncate=False)
	print(f"{'='*LINE_LENGTH}{RESET}")

	print(f"{GREEN}Unique values count for each column:")
	for column in df.columns:
		unique_count = df.select(column).distinct().count()
		print(f"{column: <20}: {unique_count:,} unique values")
	print(f"{'='*LINE_LENGTH}{RESET}")
