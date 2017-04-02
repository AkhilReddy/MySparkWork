def fl(df,i):
	func = lambda s: s[:1].lower() + s[1:] if s else ''
	x=func(df[i])
	return x

def flower(df,*args):
	df1=NulltoZero(df)
	df1=df1.rdd
	for arg in args:
		df1=df1.map(lambda x:fl(x,arg))
	return df1