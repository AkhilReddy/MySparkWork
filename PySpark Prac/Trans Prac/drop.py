def isnotNull(df,*args):
	for arg in args:
		df=df.drop(arg)
	return df