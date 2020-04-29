import re

f=open('content_a','r')
content=f.read()
f.close()
#print(content)

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