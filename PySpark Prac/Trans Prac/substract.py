def funsub(df,i,j):
	try:
		x1=int(df[i])
	except ValueError:
		x1=0
	try:
		x2=int(df[j])
	except ValueError:
		x2=0
	if x2>x1:
		x2,x1=x1,x2
	x=x1-x2
	return x

def sub(df,arg1,arg2):
	df1=df.fillna(0)
	df1=df1.fillna('')
	df1=df1.rdd
	df1=df1.map(lambda x:funsub(x,arg1,arg2))
	return df1