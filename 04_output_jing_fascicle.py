#紀錄text資料表跟fascicle資料表之間的關係

from datetime import datetime
import mysql.connector
#from MySQLdb.cursors import SSCursor
# import json

if __name__ == '__main__':
	# conn = MySQLdb.connect(host='localhost',
						   # db='taisho_juan_aligment',
						   # user='root',
						   # passwd='',
						   # charset='utf8')
						   
	conn = mysql.connector.connect(user='root', passwd='',host='localhost', db="cbeta_text_reuse_over20_high")
			 
	#cursor = conn.cursor(cursorclass=SSCursor)
	cursor = conn.cursor()
	cursor.execute("SET NAMES utf8mb4")
	cursor.execute("set character_set_server = utf8mb4")
	cursor.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令


	sql = '''select count(*) 
	FROM fascicle AS f 
	join text as t on t.id= f.text_id'''
	cursor.execute(sql)
	row = cursor.fetchone()
	rowcount=row[0]
	print ("共{}筆".format(rowcount))
	#limit 3500000
	# input(">>>")
	sql = '''select f.id,CONCAT(t.collection,LPAD(t.textnum, 4, '0'),t.associate)
	FROM fascicle AS f 
	join text as t on t.id= f.text_id'''
	cursor.execute(sql)
	
	fas_dic = {}
	try:
		current=0
		while True:
			rows = cursor.fetchmany(size=10000)
			print("({}/{})--OK".format(current,rowcount))
			if not rows:
				break
			# process your rows here.
			for row in rows:
				fas_id,jing_no = row
				key = jing_no
				value = fas_id
				# 將結果存入reuse_dict
				if key not in fas_dic:
					fas_dic[key] = list()
					fas_dic[key].append(value)
				else:
					fas_dic[key].append(value)
		#紀錄text資料表跟fascicle資料表之間的關係
		fw = open("out/jing_fascicle.txt",'w+')
		fw.write(str(fas_dic))      #把字典转化为str
		fw.close()
	finally:  
		# 要記得關 cursor、connection, 
		# 不然用 SSCursor 時會出現 warning
		#cursor.close()
		conn.commit()
		conn.close()