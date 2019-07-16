from urllib import request
import os,sys,csv,json,re,zipfile
import requests
from datetime import *

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

def getDceZipData(date,filename):
	postdata={}
	postdata["memberDealPosiQuotes.variety"] = "a"
	postdata["memberDealPosiQuotes.trade_type"] = "0"
	postdata["year"] = date[:4]
	postdata["month"] = str(int(date[4:6])-1)
	postdata["day"] = date[6:]
	postdata["contract.contract_id"] = "all"
	postdata["contract.variety_id"] = "a"
	postdata["batchExportFlag"] = "batch";
	response=requests.post("http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html",data=postdata)
	content =  response
	f = open(filename,"wb")
	f.write(response.content)
	f.close()

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

def parseDCE(datafile):
	getDceZipData("20190708",datafile)
	fzip = zipfile.ZipFile(datafile,'r')
	for file in fzip.namelist():
		fzip.extract(file,"./dce")
	filelist = os.listdir("./dce")
	sys.exit()
	products = []
	for i in range(0,len(filelist)):
		path = os.path.join("./dce",filelist[i])
		if os.path.isfile(path):
			#print(path)
			file = open(path,'r')
			lines = file.readlines()
			contract = ""
			datatype = ""
			exchange = "dce"
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
	products = ["J"]
	for product in products:
		#content = getDcePostData("20190708",product)
		#f=open("content",'w')
		#f.write(content)
		#f.close()
		f=open("content",'r')
		content=f.read()
		f.close()
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
					volume2 = bigint(match3.groupdict()["Volume2"])
					increment2 = bigint(match3.groupdict()["Diff2"])
					datatype2 = "buy"
					volume3 = bigint(match3.groupdict()["Volume3"])
					increment3 = bigint(match3.groupdict()["Diff3"])
					datatype3 = "sell"
                            

def parseCZCE(datafile):
	date = '20190708'
	file = open(datafile,'r')
	lines = file.readlines()
	isProduct = False
	for line in lines:
		exchange = "czce"
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
				product = contract
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
				rank2 = rank
				volume2 = bigint(items[5])
				increment2 = bigint(items[6])
				datatype2 = "buy"
				rank3 = rank
				volume3 = bigint(items[8])
				increment3 = bigint(items[9])
				datatype3 = "sell"

def parseCFFEX(datafile):
	#codes = ["IF", "IC", "IH", "TS", "TF", "T"]
	#for code in codes:
	date = '20190708'
	file = open(datafile,'r',encoding='GBK')
	lines = file.readlines()
	#print(lines)
	dictTotal = {}
	for line in lines:
		if not line.startswith('20') or line.strip()=="":
			continue
		items = line.split(',')
		if len(items) == 9:
			contract = items[1].strip()
			product = contract
			exchange = "cffex"
			institution = items[2].strip()
			if institution == "期货公司":
				rank = -1
			elif institution == "非期货公司":
				rank = 0
			volume = int(items[3])
			increment = int(items[4])
			datatype = "trade"
			institution2 = institution;
			volume2 = int(items[5])
			increment2 = int(items[6])
			datatype2 = "buy"
			institution3 = institution
			volume3 = int(items[7])
			increment3 = int(items[8])
			datatype3 = "sell"
		elif len(items) == 12:
			contract = items[1].strip()
			product = contract
			exchange = "cffex"
			institution = items[3].strip()
			rank = int(items[2])
			volume = int(items[4])
			increment = int(items[5])
			datatype = "trade"
			institution2 = items[6].strip()
			volume2 = int(items[7])
			increment2 = int(items[8])
			datatype2 = "buy"
			institution3 = items[9].strip()
			volume3 = int(items[10])
			increment3 = int(items[11])
			datatype3 = "sell"
			if contract not in dictTotal:
				dictTotal[contract] = [0,0,0,0,0,0]
			dictTotal[contract][0] += volume
			dictTotal[contract][1] += increment
			dictTotal[contract][2] += volume2
			dictTotal[contract][3] += increment2
			dictTotal[contract][4] += volume3
			dictTotal[contract][5] += increment3
		print(dictTotal)

def parseSHFE(date,datafile):
	file = open(datafile,'r')
	line = file.read()
	m=re.match(r'.*(?P<json>\[.*\]).*',line)
	#print(m.groupdict()['json'])
	result=json.loads(m.groupdict()['json'])
	date = datafile[-12:-4]
	print(result)
	csv_write = csv.writer(open("shfe_"+date+'.csv','w+'))
	header = ["date","exchange","product","contract","institution","rank","type","volume","increment"]
	header.extend(["institution2","rank2","type2","volume2","increment2"])
	header.extend(["institution3","rank3","type3","volume3","increment3"])
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
		row = [date,exchange,product,contract,institution,rank,datatype,volume,increment]
		institution2 = item["PARTICIPANTABBR2"].strip()
		if institution2 == "":
			institution2 = "合计"
		volume2 = item["CJ2"]
		increment2 = item["CJ2_CHG"]
		datatype2= "buy"
		rank2 = rank
		row.extend([institution2,rank2,datatype2,volume2,increment2])

		institution3 = item["PARTICIPANTABBR3"].strip()
		if institution3 == "":
			institution3 = "合计"
		volume3 = item["CJ3"]
		increment3 = item["CJ3_CHG"]
		datatype3= "sell"
		rank3 = rank
		row.extend([institution3,rank3,datatype3,volume3,increment3])
		csv_write.writerow(row)
		print(volume3)
		print(contract)
		print(institution)
	#print(line)

if __name__ == "__main__":
	today = date.today().strftime('%Y%m%d')
	today = '20190715'
	print(today)
	url_SHFE = 'http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat'%today
	#url = 'http://www.cffex.com.cn/sj/ccpm/201907/08/IF_1.csv'
	#url = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190708/FutureDataHolding.txt"
	#request.urlretrieve(url,"./czce.txt")

	#request.urlretrieve(url_SHFE, url_SHFE.split('/')[-1])
	#print(url_SHFE.split('/')[-1])
	#parseSHFE(url_SHFE.split('/')[-1])
	parseSHFE(today,'pm20190715.dat')

	#parseCFFEX("./IF_1.csv")
	#parseCZCE("./czce.txt")
	#parseDCE("./20190708_DCE_DPL.zip")