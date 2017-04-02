def NulltoZero(df,*args):
	a=list(args)
	df=df.fillna(0,subset=a)
	return df