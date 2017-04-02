def sp(df,i,ke):
	try:
		int(df[i])
		x=''
	except ValueError:
		x=df[i].split(ke)
	return x

def splt(df,arg,val):
	df1=df.na.fill('')
	df1=df1.na.fill(0)
	df1=df1.map(lambda x:sp(x,arg,val))
	return df1
