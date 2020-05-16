import re,csv,sys
import pandas as pd
from datetime import *
from getFuturePositionRank import bigint
header = ["date","product","contract","exchange","institution","rank","volume","increment","type"]

file_sgd = "/home/xiaodong/downloads/vorank_dce_sgd.csv"
file_mine = "/home/xiaodong/gitrepo/alternativeData/vorank_dce.csv"

df1 = pd.read_csv(file_sgd,encoding = "gbk")
df2 = pd.read_csv(file_mine,encoding = "gbk")

sgd = set([tuple(x) for x in df1.values])
mine = set([tuple(x) for x in df2.values])

diff1_sgd = list(sgd.difference(mine))
diff1_mine = list(mine.difference(sgd))

diff1_sgd.sort(key=lambda x:(x[1],x[2]))
diff1_mine.sort(key=lambda x:(x[1],x[2]))

print(diff1_sgd)
print(len(diff1_sgd))
print(diff1_mine)
print(len(diff1_mine))

#print(sys.getdefaultencoding())

#f_log=open('test.log','w+',encoding='GBK')

"""
f=open('content_a','r',encoding='GBK')
content=f.read()
f.close()
#print(content)
csv_write = csv.writer(open("test_dce.csv",'w+',encoding='GBK',newline=''))
csv_write.writerow(header)
product = "a"
date = '20200415'
allrows = []
strPattern22 = r"<tr >\s{1,8}<td >[0-9]{1,8}</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td>\s+"
strPattern22 += r"<td  class=\"td-right\">[0-9,]+</td>\s+<td >[0-9]+</td><td >.{2,20}</td>\s+"
strPattern22 += r"<td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>\s+<td >[0-9]+</td>\s+"
strPattern22 += r"<td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>\s+</tr>"
'''
strPattern1 = r".*<th> 名次</th>(?P<table>.*)</table>.*"
match1 = re.match(strPattern1,content)
if match1:
	table = match1.groupdict()["table"]
	#print(table)
	table2 = "<tr >      <td >1</td><td >海通期货</td><td  class=\"td-right\">13,503</td>\
	<td  class=\"td-right\">7,666</td>             <td >1</td><td >海通期货</td>\
	<td  class=\"td-right\">6,200</td><td  class=\"td-right\">743</td>              <td >1</td>\
	<td >中国国际</td><td  class=\"td-right\">3,237</td><td  class=\"td-right\">601</td>              </tr>"
	strPattern2 = r"<tr >\s{1,8}<td >[0-9]{1,4}</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td>\
	<td  class=\"td-right\">[0-9,]+</td>\s+<td >[0-9]+</td><td >.{2,20}</td>\
	<td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>\s+<td >[0-9]+</td>\
	<td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>\s{1,8}</tr>"
	match2 = re.findall(strPattern2,table2)
	print(f"match2:{len(match2)}")

	table2 = "<tr >      <td >1</td><td >海通期货</td><td  class=\"td-right\">13,503</td>\
	<td  class=\"td-right\">7,666</td>             <td >1</td><td >海通期货</td>\
	<td  class=\"td-right\">6,200</td><td  class=\"td-right\">743</td>              <td >1</td>\
	<td >中国国际</td><td  class=\"td-right\">3,237</td><td  class=\"td-right\">601</td>              </tr>"
	strPattern2 = r"<tr >\s{1,8}<td >[0-9]{1,4}</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td>\s+"
	strPattern2 += r"<td  class=\"td-right\">[0-9,]+</td>\s+<td >[0-9]+</td><td >.{2,20}</td>\s+"
	strPattern2 += r"<td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>\s+<td >[0-9]+</td>\s+"
	strPattern2 += r"<td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>\s+</tr>"
	match2 = re.findall(strPattern2,table2)
	print(f"match2:{len(match2)}")
	for item in match2:
		print(item)
		strPattern3 = r"<tr >\s+"
		strPattern3 += r"<td >(?P<No1>[0-9]+)</td><td >(?P<Name1>.{2,20})</td><td  class=\"td-right\">(?P<Volume1>[0-9,]+)</td>\s+<td  class=\"td-right\">(?P<Diff1>[0-9,]+)</td>\s+"
		strPattern3 += r"<td >(?P<No2>[0-9]+)</td><td >(?P<Name2>.{2,20})</td>\s+<td  class=\"td-right\">(?P<Volume2>[0-9,]+)</td><td  class=\"td-right\">(?P<Diff2>[0-9,]+)</td>\s+"
		strPattern3 += r"<td >(?P<No3>[0-9]+)</td>\s+<td >(?P<Name3>.{2,20})</td><td  class=\"td-right\">(?P<Volume3>[0-9,]+)</td><td  class=\"td-right\">(?P<Diff3>[0-9,]+)</td>\s+</tr>"
		#strPattern3 += r"<td >(?P<No3>[0-9]+)</td>\s+<td >(?P<Name3>.{2,20})</td>\s+<td  class=\"td-right\">(?P<Volume3>[0-9,]+)</td><td  class=\"td-right\">(?P<Diff3>[0-9,]+)</td>\s+</tr>"
		match3 = re.match(strPattern3,item)
		print(match3)
		print(match3.groupdict())
'''
strPattern1 = r".*<th> 名次</th>(?P<table>.*)</table>.*"
match1 = re.match(strPattern1,content)
if match1:
	table = match1.groupdict()["table"]
	#f_log.write(f"{table}\r\n")
	#strPattern2 = "<tr >      <td >[0-9]+</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>             \
#<td >[0-9]+</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>              \
#<td >[0-9]+</td><td >.{2,20}</td><td  class=\"td-right\">[0-9,]+</td><td  class=\"td-right\">[0-9,]+</td>              </tr>"
	strPattern2=r"<tr >\s{1,10}<td\s?>.{1,10}</td><td\s?>.{2,20}</td><td\s{1,10}class=\"td-right\">[0-9,]+</td><td\s{1,10}class=\"td-right\">[0-9,]+</td>\s+"
	strPattern2+=r"<td\s?>.{1,10}</td><td\s?>.{2,20}</td><td\s{1,10}class=\"td-right\">[0-9,]+</td><td\s{1,10}class=\"td-right\">[\d,-]+</td>\s+"
	strPattern2+=r"<td\s?>.{1,10}</td><td\s?>.{2,20}</td><td\s{1,10}class=\"td-right\">[0-9,]+</td><td\s{1,10}class=\"td-right\">[\d,-]+</td>\s{1,16}</tr>"
	match2 = re.findall(strPattern2,table)
	print(len(match2))
	for item in match2:
		strPattern3=r"<tr >\s{1,10}<td\s?>(?P<No1>.{1,10})</td><td\s?>(?P<Name1>.{2,20})</td><td\s{1,10}class=\"td-right\">(?P<Volume1>[0-9,]+)</td><td\s{1,10}class=\"td-right\">(?P<Diff1>[0-9,]+)</td>\s+"
		strPattern3+=r"<td\s?>(?P<No2>.{1,10})</td><td\s?>(?P<Name2>.{2,20})</td><td\s{1,10}class=\"td-right\">(?P<Volume2>[0-9,]+)</td><td\s{1,10}class=\"td-right\">(?P<Diff2>[\d,-]+)</td>\s+"
		strPattern3+=r"<td\s?>(?P<No3>.{1,10})</td><td\s?>(?P<Name3>.{2,20})</td><td\s{1,10}class=\"td-right\">(?P<Volume3>[0-9,]+)</td><td\s{1,10}class=\"td-right\">(?P<Diff3>[\d,-]+)</td>\s{1,16}</tr>"
		#print(item)
		match3 = re.match(strPattern3,item)
		#print(match3.groupdict())
		#date,product
		contract = "all"
		exchange = "dce"
		#{'No1': '20', 'Name1': '宏源期货', 'Volume1': '1,564', 'Diff1': '237', 'No2': '20', 'Name2': '安粮期货', 'Volume2': '1,449', 'Diff2': '72', 'No3': '20', 'Name3': '国富期货', 'Volume3': '1,126', 'Diff3': '92'}

		print(match3.groupdict()["No1"])
		if "总计".encode('gbk') == match3.groupdict()["No1"].encode('gbk'):
			institution = "合计"
			institution2 = institution
			institution3 = institution
			print(match3.groupdict())
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
allrows.sort(key=lambda x:x[1]+x[2])
#print(allrows[:5])
for row in allrows:#if int(row[6])>0:
	csv_write.writerow(row)
#print(allrows)
"""