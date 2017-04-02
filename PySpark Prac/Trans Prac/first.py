def first(df,arg):
	return df.select(df[arg]).first()