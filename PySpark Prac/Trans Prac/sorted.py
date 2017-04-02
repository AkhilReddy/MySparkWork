def sortt(df,*args):
	df1=df.na.fill(0)
	df1=df1.na.fill('')
	for arg in args:
		df1=df1.select(arg).collect()
		df1=sorted(df1)
	return df1