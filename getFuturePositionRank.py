from urllib import request
import os,sys,csv,json,re,zipfile,glob,logging
import requests,time
from datetime import datetime,date
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine,exc
from dboperMysql import MysqlDB
from dbroutine import replaceItems,insertItems

logger = logging.getLogger()
log_path = os.getcwd() + '/logs/'
log_name = os.path.join(log_path,time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(time.time())) + '.log')
fh = logging.FileHandler(log_name, mode='w')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
header = ["date","product","contract","exchange","institution","rank","volume","increment","type"]
czce_start_date = "20100825"
czce_url_change_date = "20151001"

def bigint(strBigInt):
	strInt = strBigInt.strip()
	if strInt == '-':
		return 0
	else:
		return int(float(strInt.replace(',','')))

def getProductName(contract):
	print(contract)
	if contract[-3:]=="all":
		return contract[:-3].upper()
	strPattern = r"(?P<product>[A-Za-z]+)\d+"
	m=re.match(strPattern,contract)
	if not m:
		return None
	product = m.groupdict()["product"].upper()
	if product == 'TA':
		product = 'PTA'
	return product

def getDceZipData(url,date,filename):
	postdata={}
	postdata["memberDealPosiQuotes.variety"] = "a"
	postdata["memberDealPosiQuotes.trade_type"] = "0"
	postdata["year"] = date[:4]
	postdata["month"] = str(int(date[4:6])-1)
	postdata["day"] = date[6:]
	postdata["contract.contract_id"] = "all"
	postdata["contract.variety_id"] = "a"
	postdata["batchExportFlag"] = "batch";
	response=requests.post(url,data=postdata)
	content =  response
	f = open(filename,"wb")
	f.write(response.content)
	f.close()

def downloadFile(url, filename):
	req = requests.get(url)
	if req.status_code == 404:
		print("url %s does not exist!!!"%url)
		return
	f = open(filename, 'wb')
	f.write(req.content)

def getDcePostData(date,code):
	postdata={}
	postdata["memberDealPosiQuotes.variet"] = code
	postdata["memberDealPosiQuotes.trade_type"] = "0"
	postdata["year"] = date[:4]
	postdata["month"] = str(int(date[4:6])-1)
	postdata["day"] = date[6:]
	postdata["contract.contract_id"] = "all"
	postdata["contract.variety_id"] = code
	postdata["contract"] = ""
	#responseString = Encoding.UTF8.GetString(response).Replace("\t", string.Empty).Replace("\r", string.Empty).Replace("\n", string.Empty);
	response=requests.post("http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html",data=postdata)
	content =  response.text.replace("\t","").replace("\r","").replace("\n","")
	return content

def parseDCE(date, datafile):
	fzip = zipfile.ZipFile(datafile,'r')
	for file in fzip.namelist():
		fzip.extract(file,os.path.dirname(datafile))
	filelist = glob.glob(f"{os.path.dirname(datafile)}/*.txt")
	csv_write = csv.writer(open(os.path.join(parseddir,"vorank_dce.csv"),'w+',encoding='GBK',newline=''))
	csv_write.writerow(header)
	allrows = []
	products = []
	exchange = "dce"
	for i in range(0,len(filelist)):
		#path = os.path.join(os.path.dirname(datafile),filelist[i])
		if os.path.isfile(filelist[i]):
			#print(path)
			file = open(filelist[i],'r',encoding='utf-8')
			lines = file.readlines()
			#contract = ""
			contract = filelist[i].split("_")[1]
			datatype = ""
			for line in lines:
				if "会员类别" in line:
					break
				elif line.startswith(" ") or line.startswith("\t") or line.strip() == "":
					continue
				elif "合约代码" in line:
					strPattern = r"合约代码" + r"\s*\W?\s*(?P<contract>[A-Za-z0-9]{2,6})\s+.*"
					m=re.match(strPattern,line)
					contract = m.groupdict()["contract"]
					continue
				elif line.startswith("名次"):
					if "成交量" in line:
						datatype = "trade"
					elif "持买单量" in line:
						datatype = "buy"
					elif "持卖单量" in line:
						datatype = "sell"
					continue
				else:
					items = line.replace("\t", " ").strip().split()
					if len(items) <=1:
						continue
					product = getProductName(contract)
					if product is None:
						logger.error(f'getProductName fail,date:{date},contract:{contract},file:{filelist[i]}')
						return
					product = product.lower()
					if product not in set(products):
						products.append(product)
						print(products)
					if line.startswith("总计"):
						institution = "合计"
						rank = 999
						volume = bigint(items[1])
						increment = bigint(items[2])  if len(items) > 2 else 0
					else:
						institution = items[1].strip()
						rank = int(items[0])
						volume = bigint(items[2])
						increment = bigint(items[3])
					row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
					if row:
						#csv_write.writerow(row)
						allrows.append(row)
	#products = ["J"]
	for product in products:
		content = getDcePostData(date,product)
		f=open(os.path.join(os.path.dirname(datafile),f"content_{product}"),'w')
		f.write(content)
		f.close()
		#f=open(content,'r')
		#content=f.read()
		#f.close()
		strPattern1 = r".*<th> 名次</th>(?P<table>.*)</table>.*"
		match1 = re.match(strPattern1,content)
		if match1:
			table = match1.groupdict()["table"]
			#print(table)
			strPattern2=r"<tr >\s{1,10}<td\s?>.{1,10}</td><td\s?>.{2,20}</td><td\s{1,10}class=\"td-right\">[0-9,]+</td><td\s{1,10}class=\"td-right\">[\d,-]+</td>\s+"
			strPattern2+=r"<td\s?>.{1,10}</td><td\s?>.{2,20}</td><td\s{1,10}class=\"td-right\">[0-9,]+</td><td\s{1,10}class=\"td-right\">[\d,-]+</td>\s+"
			strPattern2+=r"<td\s?>.{1,10}</td><td\s?>.{2,20}</td><td\s{1,10}class=\"td-right\">[0-9,]+</td><td\s{1,10}class=\"td-right\">[\d,-]+</td>\s{1,16}</tr>"
			match2 = re.findall(strPattern2,table)
			print(len(match2))
			for item in match2:
				strPattern3=r"<tr >\s{1,10}<td\s?>(?P<No1>.{1,10})</td><td\s?>(?P<Name1>.{2,20})</td><td\s{1,10}class=\"td-right\">(?P<Volume1>[0-9,]+)</td><td\s{1,10}class=\"td-right\">(?P<Diff1>[\d,-]+)</td>\s+"
				strPattern3+=r"<td\s?>(?P<No2>.{1,10})</td><td\s?>(?P<Name2>.{2,20})</td><td\s{1,10}class=\"td-right\">(?P<Volume2>[0-9,]+)</td><td\s{1,10}class=\"td-right\">(?P<Diff2>[\d,-]+)</td>\s+"
				strPattern3+=r"<td\s?>(?P<No3>.{1,10})</td><td\s?>(?P<Name3>.{2,20})</td><td\s{1,10}class=\"td-right\">(?P<Volume3>[0-9,]+)</td><td\s{1,10}class=\"td-right\">(?P<Diff3>[\d,-]+)</td>\s{1,16}</tr>"
				match3 = re.match(strPattern3,item)
				print(match3.groupdict())
				#date,product
				contract = "all"
				exchange = "dce"
				#{'No1': '20', 'Name1': '宏源期货', 'Volume1': '1,564', 'Diff1': '237', 'No2': '20', 'Name2': '安粮期货', 'Volume2': '1,449', 'Diff2': '72', 'No3': '20', 'Name3': '国富期货', 'Volume3': '1,126', 'Diff3': '92'}

				if "总计".encode('gbk') == match3.groupdict()["No1"].encode('gbk'):
					institution = "合计"
					institution2 = institution
					institution3 = institution
					rank = 999
				else:
					institution = match3.groupdict()["Name1"]
					institution2 = match3.groupdict()["Name2"]
					institution3 = match3.groupdict()["Name3"]
					rank = int(match3.groupdict()["No1"])
				volume = bigint(match3.groupdict()["Volume1"])
				increment = bigint(match3.groupdict()["Diff1"])
				datatype = "trade"
				row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
				if row:
					#csv_write.writerow(row)
					allrows.append(row)
				volume2 = bigint(match3.groupdict()["Volume2"])
				increment2 = bigint(match3.groupdict()["Diff2"])
				datatype2 = "buy"
				row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
				if row:
					#csv_write.writerow(row)
					allrows.append(row)
				volume3 = bigint(match3.groupdict()["Volume3"])
				increment3 = bigint(match3.groupdict()["Diff3"])
				datatype3 = "sell"
				row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
				if row:
					#csv_write.writerow(row)
					allrows.append(row)
	allrows.sort(key=lambda x:(x[1],x[2],x[5],x[8]))
	#print(allrows[:5])
	for row in allrows:#if int(row[6])>0:
		csv_write.writerow(row)

def parseCZCE(date, datafile):
	file = open(datafile,'r',encoding='utf-8')
	lines = file.readlines()
	csv_write = csv.writer(open(os.path.join(parseddir,"vorank_czce.csv"),'w+',encoding='GBK',newline=''))
	csv_write.writerow(header)
	allrows = []
	exchange = "czce"
	isProduct = False
	for line in lines:
		if line.startswith("名次") or line.strip()=="":
			continue
		if line.startswith("品种"):
			strPattern = r"品种" + r".*(?P<contract>[A-Za-z]{2,6})\s+.*"
			m=re.match(strPattern,line)
			if m:
				contract = m.groupdict()["contract"]
			else:
				contract = czce_name_code[re.split(r"：|\s",line)[1]]
			print(contract)
			product = contract
			isProduct = True
			if product=='TA':
				product = 'PTA'
			continue
		elif line.startswith("合约"):
			strPattern = r"合约" + r".*(?P<contract>[A-Za-z0-9]{5,10})\s+.*"
			m=re.match(strPattern,line)
			contract = m.groupdict()["contract"]
			print(contract)
			isProduct = False
			continue
		else:
			line = line.strip()
			if '|' in line:
				items = line.split('|')
			else:
				items = line.split(',')
			print(items)
			if len(items) != 10:
				continue
			if items[8] == "" or items[9] == "" or items[2] == "" or items[3] == "" or items[5] == "" or items[6] == "":
				continue
			if isProduct:
				contract = "all"
			else:
				product = getProductName(contract)
			if line.startswith("合计"):
				institution = "合计"
				institution2 = institution
				institution3 = institution
				rank = 999
			else:
				institution = items[1].strip()
				institution2 = items[4].strip()
				institution3= items[7].strip()
				rank = int(items[0])
			volume = bigint(items[2])
			increment = bigint(items[3])
			datatype = "trade"
			row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
			print(row)
			#if row:
			allrows.append(row)
				#csv_write.writerow(row)
			volume2 = bigint(items[5])
			increment2 = bigint(items[6])
			datatype2 = "buy"
			row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
			print(row)
			#if row:
			allrows.append(row)
				#csv_write.writerow(row)
			volume3 = bigint(items[8])
			increment3 = bigint(items[9])
			datatype3 = "sell"
			row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
			print(row)
			#if row:
			allrows.append(row)
				#csv_write.writerow(row)
	allrows.sort(key=lambda x:(x[1],x[2],x[5],x[8]))
	print(allrows[:5])
	for row in allrows:
		if int(row[6])  == 0  and int(row[7])  == 0:
			continue
		csv_write.writerow(row)

def parseCFFEX(date, code, datafile):
	file = open(datafile,'r',encoding='GBK')
	lines = file.readlines()
	#print(lines)
	bCreate=False
	if not os.path.exists(os.path.join(parseddir,"vorank_cffex.csv")):
		bCreate=True
	csv_write = csv.writer(open(os.path.join(parseddir,"vorank_cffex.csv"),'a+',encoding='GBK',newline=''))
	if bCreate:
		csv_write.writerow(header)
	allrows = []
	exchange = "cffex"
	dictTotal = {}
	for line in lines:
		if not line.startswith('20') or line.strip()=="":
			continue
		items = line.split(',')
		if len(items) == 9:
			contract = items[1].strip()
			product = getProductName(contract)
			institution = items[2].strip()
			if institution == "期货公司":
				rank = -1
			elif institution == "非期货公司":
				rank = 0
			else:
				continue
			volume = int(items[3])
			increment = int(items[4])
			datatype = "trade"
			row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
			if row:
				#csv_write.writerow(row)
				allrows.append(row)
			volume2 = int(items[5])
			increment2 = int(items[6])
			datatype2 = "buy"
			row = [date,product,contract,exchange,institution,rank,volume2,increment2,datatype2]
			if row:
				#csv_write.writerow(row)
				allrows.append(row)
			volume3 = int(items[7])
			increment3 = int(items[8])
			datatype3 = "sell"
			row = [date,product,contract,exchange,institution,rank,volume3,increment3,datatype3]
			if row:
				#csv_write.writerow(row)
				allrows.append(row)
		elif len(items) == 12:
			contract = items[1].strip()
			product = getProductName(contract)
			institution = items[3].strip()
			rank = int(items[2])
			volume = int(items[4])
			increment = int(items[5])
			datatype = "trade"
			row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
			if row:
				#csv_write.writerow(row)
				allrows.append(row)
			institution2 = items[6].strip()
			volume2 = int(items[7])
			increment2 = int(items[8])
			datatype2 = "buy"
			row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
			if row:
				#csv_write.writerow(row)
				allrows.append(row)
			institution3 = items[9].strip()
			volume3 = int(items[10])
			increment3 = int(items[11])
			datatype3 = "sell"
			row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
			if row:
				#csv_write.writerow(row)
				allrows.append(row)
			if contract not in dictTotal:
				dictTotal[contract] = [0,0,0,0,0,0]
			dictTotal[contract][0] += volume
			dictTotal[contract][1] += increment
			dictTotal[contract][2] += volume2
			dictTotal[contract][3] += increment2
			dictTotal[contract][4] += volume3
			dictTotal[contract][5] += increment3
		print(dictTotal)
	for contract in dictTotal:
		row = [date,getProductName(contract),contract,exchange,"合计",999,dictTotal[contract][0],dictTotal[contract][1],"trade"]
		if row:
			#csv_write.writerow(row)
			allrows.append(row)
		row = [date,getProductName(contract),contract,exchange,"合计",999,dictTotal[contract][2],dictTotal[contract][3],"buy"]
		if row:
			#csv_write.writerow(row)
			allrows.append(row)
		row = [date,getProductName(contract),contract,exchange,"合计",999,dictTotal[contract][4],dictTotal[contract][5],"sell"]
		if row:
			#csv_write.writerow(row)
			allrows.append(row)
	allrows.sort(key=lambda x:x[1]+x[2])
	print(allrows[:5])
	for row in allrows:
		#if int(row[6])>0:
		csv_write.writerow(row)

def parseSHFE(date,datafile):
	file = open(datafile,'r',encoding='utf-8')
	line = file.read()
	m=re.match(r'.*(?P<json>\[.*\]).*',line)
	#print(m.groupdict()['json'])
	result=json.loads(m.groupdict()['json'])
	date = datafile[-12:-4]
	print(result)
	csv_write = csv.writer(open(os.path.join(parseddir,"vorank_shfe.csv"),'w+',encoding='GBK',newline=''))
	csv_write.writerow(header)
	allrows = []
	for item in result:
		print(item)
		contract = item["INSTRUMENTID"].strip()
		if "actv" in contract:
			logger.error(f'getProductName fail,date:{date},contract:{contract}')
			continue
		product = getProductName(contract)
		if product is None:
			logger.error(f'getProductName fail,date:{date},contract:{contract}')
			return
		product = product.lower()
		exchange = "shfe"
		institution = item["PARTICIPANTABBR1"].strip()
		if institution == "":
			institution = "合计"
		
		rank = int(item["RANK"])
		if rank<=0 and "all" in contract:
			contract = "all"
		volume = item["CJ1"]
		increment = item["CJ1_CHG"]
		datatype = "trade"
		row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
		if row:
			#csv_write.writerow(row)
			allrows.append(row)
		institution2 = item["PARTICIPANTABBR2"].strip()
		if institution2 == "":
			institution2 = "合计"
		volume2 = item["CJ2"]
		increment2 = item["CJ2_CHG"]
		datatype2= "buy"
		row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
		if row:
			#csv_write.writerow(row)
			allrows.append(row)

		institution3 = item["PARTICIPANTABBR3"].strip()
		if institution3 == "":
			institution3 = "合计"
		volume3 = item["CJ3"]
		increment3 = item["CJ3_CHG"]
		datatype3= "sell"
		row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
		if row:
			#csv_write.writerow(row)
			allrows.append(row)
		print(volume3)
		print(contract)
		print(institution)
	#print(line)
	allrows.sort(key=lambda x:x[1]+x[2])
	#print(allrows[:5])
	for row in allrows:
		csv_write.writerow(row)

def vorank2db(tradedate):
	db_op.dbConnect(config['db_market'])
	curdatepath = "./parsed/" + tradedate
	filelist = glob.glob(f"{curdatepath}/*.csv")
	print(filelist)

	for csvfile in filelist:
		df = pd.read_csv(csvfile,encoding='GBK',engine='python',na_filter=False,dtype={'date': str, 'rank': str,'volume':str,'increment':str})
		print(df.dtypes)
		print(df[df.isnull().T.any()])
		print(df[:5])
		#df['date']=df['date'].apply(str)
		print(df.shape)
		df.drop_duplicates(inplace=True)
		print(df.shape)
		df.to_sql(f'vorank_{tradedate}', engine, if_exists='replace',index=False)
		#del df['rank']
		columns = df.columns.values.tolist()
		#values =[tuple(x) for x in df.values[-1:]]
		#values = tuple(map(tuple, df.values))
		#replaceItems(db_op,'vorank',columns,values)
		copy_sql = f"replace into vorank select * from vorank_{tradedate}"
		db_op.dbExecute(copy_sql)

if __name__ == "__main__":
	codes_cffex = ["IC", "IF", "IH", "T", "TF", "TS"]
	today = date.today().strftime('%Y%m%d')
	#print(today)

	'''with open("config.json") as f:
		config = json.load(f)["db"]
	db_op = MysqlDB(config['user'],config['password'],config['host'])
	connStr = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}/{config['db_market']}?charset=utf8"
	engine = create_engine(connStr, encoding='utf-8', echo=True)'''
	#print(config)

	czce_name_code = {}
	df_czce = pd.read_csv("czce.csv")
	for idx,row in df_czce.iterrows():
		czce_name_code[row["name"]] = row["code"]

	startdate = today
	enddate = today

	#df = pd.read_csv('calendar_all.csv')
	#alldates = df.tradedate.values.tolist()
	#alldates = [str(x) for x in alldates if str(x) >= startdate and str(x) <= enddate]
	alldates=[today]
	#print(alldates)

	'''for tradedate in alldates:
		db_op.dbConnect(config['db_market'])
		delete_sql = f"drop table vorank_{tradedate}"
		db_op.dbExecute(delete_sql)
	input()'''

	for tradedate in alldates:
		parseddir = "./parsed/" + tradedate
		if not os.path.exists(parseddir):
			os.makedirs(parseddir)

		datadir_cffex = "./cffex/"  + tradedate
		if not os.path.exists(datadir_cffex):
			os.makedirs(datadir_cffex)
		for code in codes_cffex:
			url_CFFEX = 'http://www.cffex.com.cn/sj/ccpm/%s/%s/%s_1.csv'%(tradedate[:6],tradedate[6:],code)
			data_cffex = os.path.join(datadir_cffex,url_CFFEX.split('/')[-1].split(".")[0]+"_"+tradedate+".csv")
			request.urlretrieve(url_CFFEX, data_cffex)
			print(url_CFFEX.split('/')[-1].split(".")[0]+"_"+tradedate+".csv")
			parseCFFEX(tradedate,code,data_cffex)

		datadir_czce = "./czce/"  + tradedate
		if not os.path.exists(datadir_czce):
			os.makedirs(datadir_czce)
		if tradedate >= czce_url_change_date:
			url_czce = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataHolding.txt"%(tradedate[:4],tradedate)
		else:
			url_czce = "http://www.czce.com.cn/cn/exchange/%s/datatradeholding/%s.txt"%(tradedate[:4],tradedate)
		print(url_czce)
		file_czce = os.path.join(datadir_czce,url_czce.split('/')[-1].split(".")[0] + "_" + tradedate + ".txt")
		#request.urlretrieve(url_czce, file_czce)
		if not os.path.exists(file_czce):
			downloadFile(url_czce, file_czce)
		parseCZCE(tradedate, file_czce)

		datadir_dce = "./dce/"  + tradedate
		if not os.path.exists(datadir_dce):
			os.makedirs(datadir_dce)
		url_dce = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
		data_dce = os.path.join(datadir_dce,tradedate+"_DCE_DPL.zip")
		if not os.path.exists(data_dce):
			getDceZipData(url_dce, tradedate, data_dce)
		parseDCE(tradedate, data_dce)

		datadir_shfe = "./shfe/"  + tradedate
		if not os.path.exists(datadir_shfe):
			os.makedirs(datadir_shfe)
		url_SHFE = 'http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat'%tradedate
		data_shfe = os.path.join(datadir_shfe,url_SHFE.split('/')[-1])
		if not os.path.exists(data_shfe):
			request.urlretrieve(url_SHFE, data_shfe)
		print(url_SHFE.split('/')[-1])
		parseSHFE(tradedate, data_shfe)

		#vorank2db(tradedate)
