def funmul(df,i,j):
	try:
		x1=int(df[i])
	except ValueError:
		x1=1
	try:
		x2=int(df[j])
	except ValueError:
		x2=1
	x=x1*x2
	return x

def mul(df,arg1,arg2):
	df1=df.fillna(1)
	df1=df1.rdd
	df1=df1.map(lambda x:funmul(x,arg1,arg2))
	return df1