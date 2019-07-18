from urllib import request
import os,sys,csv,json,re,zipfile
import requests
from datetime import *

header = ["date","exchange","product","contract","institution","rank","type","volume","increment"]
header = ["date","product","contract","exchange","institution","rank","volume","increment","type"]

def bigint(strBigInt):
	strInt = strBigInt.strip()
	if strInt == '-':
		return 0
	else:
		return int(strInt.replace(',',''))

def getProductName(contract):
	print(contract)
	if contract[-3:]=="all":
		return contract[:-3].upper()
	strPattern = r"(?P<product>[A-Za-z]+)\d+"
	m=re.match(strPattern,contract)
	product = m.groupdict()["product"].upper()
	if product == 'TA':
		product = 'PTA'
	#rint(product)
	return product
	pass

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
		fzip.extract(file,"./dce")
	filelist = os.listdir("./dce")
	csv_write = csv.writer(open("vorank_dce.csv",'w+',encoding='GBK'))
	csv_write.writerow(header)
	products = []
	exchange = "dce"
	for i in range(0,len(filelist)):
		path = os.path.join("./dce",filelist[i])
		if os.path.isfile(path):
			#print(path)
			file = open(path,'r')
			lines = file.readlines()
			contract = ""
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
					csv_write.writerow(row)
	#products = ["J"]
	for product in products:
		content = getDcePostData(date,product)
		#f=open("content",'w')
		#f.write(content)
		#f.close()
		#f=open(content,'r')
		#content=f.read()
		#f.close()
		strPattern1 = r".*<th> 名次</th>(?P<table>.*)</table>.*"
		match1 = re.match(strPattern1,content)
		if match1:
			table = match1.groupdict()["table"]
			#print(table)
			strPattern2 = "<tr >      <td >[0-9]+</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>             \
<td >[0-9]+</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>              \
<td >[0-9]+</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>              </tr>"
			match2 = re.findall(strPattern2,table)
			print(len(match2))
			for item in match2:
				strPattern3 = "<tr >      <td >(?P<No1>[0-9]+)</td><td >(?P<Name1>.{2,20})</td><td  class=\"td-right\">(?P<Volume1>[0-9,]+)</td><td  class=\"td-right\">(?P<Diff1>[0-9,]+)</td>             \
<td >(?P<No2>[0-9]+)</td><td >(?P<Name2>.{2,20})</td><td  class=\"td-right\">(?P<Volume2>[0-9,]+)</td><td  class=\"td-right\">(?P<Diff2>[0-9,]+)</td>              \
<td >(?P<No3>[0-9]+)</td><td >(?P<Name3>.{2,20})</td><td  class=\"td-right\">(?P<Volume3>[0-9,]+)</td><td  class=\"td-right\">(?P<Diff3>[0-9,]+)</td>              </tr>"
				match3 = re.match(strPattern3,item)
				print(match3.groupdict())
				#date,product
				contract = "all"
				exchange = "dce"
				#{'No1': '20', 'Name1': '宏源期货', 'Volume1': '1,564', 'Diff1': '237', 'No2': '20', 'Name2': '安粮期货', 'Volume2': '1,449', 'Diff2': '72', 'No3': '20', 'Name3': '国富期货', 'Volume3': '1,126', 'Diff3': '92'}
				
				if "总计" in match3.groupdict()["No1"]:
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
					csv_write.writerow(row)
					volume2 = bigint(match3.groupdict()["Volume2"])
					increment2 = bigint(match3.groupdict()["Diff2"])
					datatype2 = "buy"
					row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
					csv_write.writerow(row)
					volume3 = bigint(match3.groupdict()["Volume3"])
					increment3 = bigint(match3.groupdict()["Diff3"])
					datatype3 = "sell"
					row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
					csv_write.writerow(row)

def parseCZCE(date, datafile):
	file = open(datafile,'r')
	lines = file.readlines()
	csv_write = csv.writer(open("vorank_czce.csv",'w+',encoding='GBK'))
	csv_write.writerow(header)
	isProduct = False
	exchange = "czce"
	for line in lines:
		if line.startswith("名次") or line.strip()=="":
			continue
		if line.startswith("品种"):
			strPattern = r"品种" + r".*(?P<contract>[A-Za-z]{2,6})\s+.*"
			m=re.match(strPattern,line)
			contract = m.groupdict()["contract"]
			print(contract)
			isProduct = True
			continue
		elif line.startswith("合约"):
			strPattern = r"合约" + r".*(?P<contract>[A-Za-z0-9]{5,10})\s+.*"
			m=re.match(strPattern,line)
			contract = m.groupdict()["contract"]
			print(contract)
			isProduct = False
			continue
		else:
			items = line.split('|')
			if len(items) != 10:
				continue
			if isProduct:
				contract = "all"
				product = contract
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
			csv_write.writerow(row)
			volume2 = bigint(items[5])
			increment2 = bigint(items[6])
			datatype2 = "buy"
			row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
			csv_write.writerow(row)
			volume3 = bigint(items[8])
			increment3 = bigint(items[9])
			datatype3 = "sell"
			row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
			csv_write.writerow(row)

def parseCFFEX(date, code, datafile):
	file = open(datafile,'r',encoding='GBK')
	lines = file.readlines()
	#print(lines)
	bCreate=False
	if not os.path.exists("vorank_cffex.csv"):
		bCreate=True
	csv_write = csv.writer(open("vorank_cffex.csv",'a+',encoding='GBK'))
	if bCreate:
		csv_write.writerow(header)
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
			volume = int(items[3])
			increment = int(items[4])
			datatype = "trade"
			row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
			csv_write.writerow(row)
			volume2 = int(items[5])
			increment2 = int(items[6])
			datatype2 = "buy"
			row = [date,product,contract,exchange,institution,rank,volume2,increment2,datatype2]
			csv_write.writerow(row)
			volume3 = int(items[7])
			increment3 = int(items[8])
			datatype3 = "sell"
			row = [date,product,contract,exchange,institution,rank,volume3,increment3,datatype3]
			csv_write.writerow(row)
		elif len(items) == 12:
			contract = items[1].strip()
			product = getProductName(contract)
			institution = items[3].strip()
			rank = int(items[2])
			volume = int(items[4])
			increment = int(items[5])
			datatype = "trade"
			row = [date,product,contract,exchange,institution,rank,volume,increment,datatype]
			csv_write.writerow(row)
			institution2 = items[6].strip()
			volume2 = int(items[7])
			increment2 = int(items[8])
			datatype2 = "buy"
			row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
			csv_write.writerow(row)
			institution3 = items[9].strip()
			volume3 = int(items[10])
			increment3 = int(items[11])
			datatype3 = "sell"
			row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype2]
			csv_write.writerow(row)
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
		row = [date,getProductName(contract),contract,exchange,"合计",999,"trade",dictTotal[contract][0],dictTotal[contract][1]]
		csv_write.writerow(row)
		row = [date,getProductName(contract),contract,exchange,"合计",999,"buy",dictTotal[contract][2],dictTotal[contract][3]]
		csv_write.writerow(row)
		row = [date,getProductName(contract),contract,exchange,"合计",999,"sell",dictTotal[contract][4],dictTotal[contract][5]]
		csv_write.writerow(row)

def parseSHFE(date,datafile):
	file = open(datafile,'r')
	line = file.read()
	m=re.match(r'.*(?P<json>\[.*\]).*',line)
	#print(m.groupdict()['json'])
	result=json.loads(m.groupdict()['json'])
	date = datafile[-12:-4]
	print(result)
	csv_write = csv.writer(open("vorank_shfe.csv",'w+',encoding='GBK'))
	csv_write.writerow(header)
	for item in result:
		print(item)
		contract = item["INSTRUMENTID"].strip()
		product = getProductName(contract)
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
		csv_write.writerow(row)
		institution2 = item["PARTICIPANTABBR2"].strip()
		if institution2 == "":
			institution2 = "合计"
		volume2 = item["CJ2"]
		increment2 = item["CJ2_CHG"]
		datatype2= "buy"
		row = [date,product,contract,exchange,institution2,rank,volume2,increment2,datatype2]
		csv_write.writerow(row)

		institution3 = item["PARTICIPANTABBR3"].strip()
		if institution3 == "":
			institution3 = "合计"
		volume3 = item["CJ3"]
		increment3 = item["CJ3_CHG"]
		datatype3= "sell"
		row = [date,product,contract,exchange,institution3,rank,volume3,increment3,datatype3]
		csv_write.writerow(row)
		print(volume3)
		print(contract)
		print(institution)
	#print(line)

if __name__ == "__main__":
	today = date.today().strftime('%Y%m%d')
	#today = '20190715'
	print(today)
	
	url_SHFE = 'http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat'%today
	request.urlretrieve(url_SHFE, url_SHFE.split('/')[-1])
	print(url_SHFE.split('/')[-1])
	parseSHFE(today, url_SHFE.split('/')[-1])
	
	codes = ["IF", "IC", "IH", "TS", "TF", "T"]
	for code in codes:
		url_CFFEX = 'http://www.cffex.com.cn/sj/ccpm/%s/%s/%s_1.csv'%(today[:6],today[6:],code)
		request.urlretrieve(url_CFFEX, url_CFFEX.split('/')[-1].split(".")[0]+"_"+today+".csv")
		print(url_CFFEX.split('/')[-1].split(".")[0]+"_"+today+".csv")
		parseCFFEX(today,code,url_CFFEX.split('/')[-1].split(".")[0]+"_"+today+".csv")
	
	url_czce = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataHolding.txt"%(today[:4],today)
	print(url_czce)
	file_czce = url_czce.split('/')[-1].split(".")[0] + "_" + today + ".txt"
	#request.urlretrieve(url_czce, file_czce)
	downloadFile(url_czce, file_czce)
	parseCZCE(today, file_czce)
	
	url_dce = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
	getDceZipData(url_dce, today, today+"_DCE_DPL.zip")
	parseDCE(today, today+"_DCE_DPL.zip")