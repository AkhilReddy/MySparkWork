def last(df,arg):
	df=df.select(arg).collect() #return type will be list
	df=df[-1]
	return df
	