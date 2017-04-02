def funadd(df,i,j):
	try:
		x1=int(df[i])
	except ValueError:
		x1=0
	try:
		x2=int(df[j])
	except ValueError:
		x2=0
	x=x1+x2
	return x

def add(df,arg1,arg2):
	df1=df.fillna(0)
	df1=df1.rdd
	df1=df1.map(lambda x:funadd(x,arg1,arg2))
	return df1