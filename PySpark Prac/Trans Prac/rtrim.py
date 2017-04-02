def rt(df,i):
	try:
		int(df[i])
		x=''
	except ValueError:
		x=df[i].rstrip()
	return x

def rtrim(df,*args):
	df1=df.fillna('')
	df1=df1.fillna(0)
	df1=df1.rdd
	for arg in args:
		df1=df1.map(lambda x:rt(x,arg))
	return df1