def dis(df,arg):
	df1=df.select(arg).distinct()
	return df1