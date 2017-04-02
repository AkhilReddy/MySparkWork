def cap(df,i):
	try:
		int(df[i])
		x=''
	except ValueError:
		x=df[i].title()
	return x

def initcap(df,*args):
	df1=df.fillna('')
	df1=df1.fillna(0)
	df1=df1.rdd
	for arg in args:
		df1=df1.map(lambda x:cap(x,arg))
	return df1