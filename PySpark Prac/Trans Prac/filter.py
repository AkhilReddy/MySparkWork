def fil(df,arg,op,val):
	df1=df.na.fill(0)
	df1=df1.na.fill('')
	df1=df1.filter(df1[arg])
	return df1