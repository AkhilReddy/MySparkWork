def group(df,arg):
	df1=df.groupBy(df.name).avg().collect()
	return df1