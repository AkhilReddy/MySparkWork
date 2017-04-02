def ZerotoNull(df,*args):
	a=list(args)
	df=df.na.replace(0,'',subset=a)
	return df