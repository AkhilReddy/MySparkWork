def lt(df,i):
	try:
		int(df[i])
		x=''
	except ValueError:
		x=df[i].lstrip()
	return x

def ltrim(df,*args):
	df1=df.fillna('')
	df1=df1.fillna(0)
	df1=df1.rdd
	for arg in args:
		df1=df1.map(lambda x:lt(x,arg))
	return df1