import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine,exc
import json,os,platform,glob
from datetime import datetime,date

today = date.today().strftime('%Y%m%d')
with open("config_zelin.json") as f:
		config = json.load(f)["db"]
connStr = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}/{config['db_market']}?charset=utf8"
engine = create_engine(connStr, encoding='utf-8', echo=True)

exchangelist = ('cffex','shfe','czce','dce')

def getSymbols():
	connStr1 = f"mysql+pymysql://dev:x.xd@Tokyo0224@{config['host']}/common?charset=utf8"
	engine1 = create_engine(connStr1, encoding='utf-8', echo=True)
	query = "select code,name from fut_contract where market= 'CZC'"
	outputpath = './'
	df = pd.read_sql(query, engine1)
	df.to_csv(os.path.join(outputpath,"czce.csv"),index=False)

def getRankbySymbol(symbol):
	query = f"select * from vorank where product= '{symbol}' and  institution='合计' order by date,contract"
	outputpath = './research'
	df = pd.read_sql(query, engine)
	df['date'] = df['date'].map(lambda x:x.strftime('%Y%m%d'))
	df.to_csv(os.path.join(outputpath,symbol+".csv"),index=False)

def appendZSZL(sourcefile,outputPath):
	outcsv = os.path.join(outputPath,sourcefile.split("/")[-1])
	print(outcsv)
	if 'IF' not in outcsv:
		return
	#if os.path.exists(outcsv):
	#	return
	df = pd.read_csv(sourcefile)
	shiftidxs = df[df['mon1'].notnull()].index.tolist()
	print(shiftidxs)
	product = sourcefile.split("/")[-1].split(".")[0]
	print(df[90:93])
	print(df[:5])
	print(int(df.loc[shiftidxs[0]]["mon1"]))
	df["contract"] = product
	for idx in range(0,len(shiftidxs)):
		if idx ==0:
			contract = int(df.loc[shiftidxs[idx]]["mon1"])
			df[:(shiftidxs[idx]+1)]['contract'] = f"{product}{contract}"
		elif idx == len(shiftidxs)-1:
			contract1 = int(df.loc[shiftidxs[idx]]["mon1"])
			df[(shiftidxs[idx-1]+1):(shiftidxs[idx]+1)]['contract'] = f"{product}{contract1}"
			contract2= int(df.loc[shiftidxs[idx]]["mon2"])
			df[(shiftidxs[idx]+1):]['contract'] = f"{product}{contract2}"
		else:
			contract = int(df.loc[shiftidxs[idx]]["mon1"])
			df[(shiftidxs[idx-1]+1):(shiftidxs[idx]+1)]['contract'] = f"{product}{contract}"

	#query = f"select * from vorank where product= '{symbol}' and  institution='合计' order by date,contract"
	query = f"select date,type,contract,sum(volume) volume from vorank where product='{symbol}' and contract!='all' and institution!='合计' "
	query += "and type!='trade' group by date,type,contract"
	df_rank = pd.read_sql(query, engine)
	df_rank['date'] = df_rank['date'].map(lambda x:x.strftime('%Y/%m/%d'))
	df_rank.rename(columns={"date":"datetime","volume":"vol"},inplace=True)
	print(df_rank)
	#df_rank = pd.read_csv(f"./research/{sourcefile.split('/')[-1]}")
	#df_rank.rename(columns={"date":"datetime","volume":"vol"},inplace=True)
	#df_rank['datetime'] = df_rank['datetime'].map(lambda x:"/".join([str(x)[:4],str(x)[4:6],str(x)[6:]]))

	df_buy = df_rank[df_rank["type"]=="buy"]
	df_buy_sum = df_buy.groupby(['datetime']).sum()
	df_buy_sum.reset_index(inplace=True)
	df_buy.rename(columns={"vol":"position_buy"},inplace=True)
	df_buy_sum.rename(columns={"vol":"position_buy_all"},inplace=True)
	df_buy = df_buy[["datetime", "contract","position_buy"]]
	print(df_buy[-5:])
	print(df_buy_sum[-5:])

	df_sell = df_rank[df_rank["type"]=="sell"]
	df_sell_sum = df_sell.groupby(['datetime']).sum()
	df_sell_sum.reset_index(inplace=True)
	df_sell.rename(columns={"vol":"position_sell"},inplace=True)
	df_sell_sum.rename(columns={"vol":"position_sell_all"},inplace=True)
	df_sell = df_sell[["datetime", "contract","position_sell"]]
	print(df_sell[-5:])
	print(df_sell_sum[-5:])

	df_merge = pd.merge(df,df_buy,on=('datetime','contract'),how="left")
	df_merge = pd.merge(df_merge,df_buy_sum,on=('datetime'),how="left")
	df_merge = pd.merge(df_merge,df_sell,on=('datetime','contract'),how="left")
	df_merge = pd.merge(df_merge,df_sell_sum,on=('datetime'),how="left")
	print(df_merge[:5])

	df_merge[['position_buy','position_sell','position_buy_all','position_sell_all']] = df_merge[['position_buy','position_sell','position_buy_all','position_sell_all']].fillna(method='ffill')

	print(df_merge[df_merge["position_buy"].isnull()])
	df_merge.to_csv(os.path.join(outputPath,sourcefile.split("/")[-1]),index=False)

def getRankFromDb(tradedate=today,exchange='all'):
	query = f"select * from vorank where date={tradedate}"
	if exchange != 'all':
		query += f" and exchange={exchange}"
	outputpath = f'./{tradedate}'
	if not os.path.exists(outputpath):
		os.mkdir(f'./{tradedate}')
	df = pd.read_sql(query, engine)
	df['date'] = df['date'].map(lambda x:x.strftime('%Y%m%d'))
	groups = df.groupby('exchange')
	for exchange,group in groups:
		print(exchange)
		group =  group.sort_values(by=['contract','rank'],ascending=(True,True))
		print(group[:5])
		if platform.system() == 'Linux':
			group.to_csv(os.path.join(outputpath,f'vorank_{exchange}.csv'),encoding='utf8',index=False)
		elif platform.system() == 'Windows':
			group.to_csv(os.path.join(outputpath,f'vorank_{exchange}.csv'),encoding='GBK',index=False)
		else:
			group.to_csv(os.path.join(outputpath,f'vorank_{exchange}.csv'),index=False)
	return df

if __name__ == '__main__':
	#getRankFromDb(tradedate='20200730')
	#getSymbols()
	#input()

	sourcepath = "/home/xiaodong/data/ZSZL/D"
	symbols = []
	for csvpath in glob.glob(f"{sourcepath}/*.csv"):
		symbols.append(csvpath.split("/")[-1].split(".")[0])

	print(symbols)

	destpath = "/home/xiaodong/data/ZSZL/D_rank"
	for symbol in symbols:
		appendZSZL(os.path.join(sourcepath, f"{symbol}.csv"), destpath)