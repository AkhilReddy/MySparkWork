def lkup(df,arg,ke):
	df1=df.select(arg).collect()
	for k in df1:
		if k[arg]==ke:
			return True
		else:
			return False