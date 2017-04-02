def uni(df1,df2):
	df3=df1.union(df2).distinct()
	return df3