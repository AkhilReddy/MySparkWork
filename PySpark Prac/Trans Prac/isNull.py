def isNull(df,*args):
	count=0
	for arg in args:
		df=df.select(arg).collect()
		for k in df:
			if k[arg]==None:
				count+=1
	return count