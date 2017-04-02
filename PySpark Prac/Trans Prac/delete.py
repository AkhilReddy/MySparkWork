def dele(df,arg,val):
	df1=df.fillna(0)
	df1=df.select(arg).collect()
	a=[]
	for k in df1:
		if k[arg]!=val:
			a.append(k)
	return a