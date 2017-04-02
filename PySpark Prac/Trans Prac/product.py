def product(df,arg):
	df1=df.fillna(1,subset=arg)
	df=df1.select(arg).collect()
	m=1
	for k in df:
		try:
			x1=int(k[arg])
		except ValueError:
			x1=1
		m=m*x1
	return m
