def colrename(df,*args):
	a=list(args)
	for k in range(int(len(a)/2)):
		r=int(len(a)/2)+k
		df=df.withColumnRenamed(a[k], a[r])
	return df