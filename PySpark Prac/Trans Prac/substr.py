def substring(df,sub,*args):
	df1=df.na.fill(0)
	df1=df1.na.fill('')
	count=0
	for arg in args:
		df1=df1.select(arg).collect()
		for k in df1:
			x=str(k[arg]).find(sub)
			if x>-1:
				count+=1
	return count