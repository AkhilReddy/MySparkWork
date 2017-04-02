def funcat(df,i,j):
	try:
		int(df[i])
		x1=''
	except ValueError:
		x1=str(df[i])
	try:
		int(df[j])
		x2=''
	except ValueError:
		x2=str(df[j])
	x=x1+x2
	return x

def concat(df,arg1,arg2):
	df1=df.fillna(0)
	df1=df1.fillna('')
	df1=df1.rdd
	df1=df1.map(lambda x:funcat(x,arg1,arg2))
	return df1