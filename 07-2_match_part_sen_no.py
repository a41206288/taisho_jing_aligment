#重算資料庫text_reuse_in_fascicles資料表的資料，
#另外還會紀錄兩卷(fas)之前參考的pair，與其match的句號(sentence_id) 存入reuse_dict_test.txt

#import MySQLdb
from datetime import datetime
import mysql.connector
import re
import MySQLdb
from MySQLdb.cursors import SSCursor
# import json
# import pickle


if __name__ == '__main__':
	conn = MySQLdb.connect(host='localhost',
						   db='cbeta_text_reuse_over20',
						   user='root',
						   passwd='',
						   charset='utf8'
						   )
						   
	# conn = mysql.connector.connect(user='root', passwd='',host='localhost', db="cbeta_text_reuse",buffered=True)
			 
	cursor = conn.cursor(cursorclass=SSCursor)

	cursor.execute("SET NAMES utf8mb4")
	cursor.execute("set character_set_server = utf8mb4")
	cursor.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令


	sql = '''select count(*) 
	FROM pair_sentence AS psa join pair AS pa on psa.pair_id = pa.id'''
	cursor.execute(sql)
	row = cursor.fetchone()
	rowcount=row[0]
	print ("共{}筆".format(rowcount))
	#limit 3500000
	# input(">>>")
	sql = '''select psa.id,sent_id,match_part,score,match_len
	FROM pair_sentence AS psa join pair AS pa on psa.pair_id = pa.id'''
	cursor.execute(sql)
	
	
	t0 = datetime.now()
				# delta = t2 - t1
				
	# with open("./match_part_sen_no.sql", "w", encoding='utf-8') as f:
		# f.write("INSERT INTO `match_part` (`id`, `sen_idA`, `sen_idB`, `match_part`, `merge`, `score`, `match_len`, merge_len) VALUES\n")
	result_list=[]
	try:
		current=0
		id = 0
		while True:
			rows = cursor.fetchmany(size=10000)
			print("({}/{})--OK".format(current,rowcount))
			if not rows:
				break
			# process your rows here.

			for row in rows:
				current+=1
				#讀取資料庫欄位
				pair_sentence_id,sent_id,match_part,score,match_len = row

				# pair_sentence中每個pair會存兩筆A和B，取id為偶數時判斷為第一筆，奇數判斷為第二筆
				if (int(pair_sentence_id) % 2) == 0:
					sen_idA = sent_id
				else:
					
					sen_idB = sent_id
					merge = ""
					merge_len = 0
					
					#2020.4.13 改成用"..."切 因為用空白切會有bug
					# m = re.split(' ',match_part)
					delimiters = "...","(C)"
					regexPattern = '|'.join(map(re.escape, delimiters))
					m = re.split(regexPattern,match_part)
					
					# print("m:{}".format(m))
					matchA = m[1].strip()
					matchB = m[3].strip()
					# print("A句:{}".format(matchA))
					# print("B句:{}".format(matchB))
					merge = ""
					merge_len = 0
					for a, b in zip(matchA, matchB):
						if a != b:
							if a == "-" or b == "-":
								#插字
								merge += "○"
							else:
								#換字
								merge += "◎"
						else:
							merge += a
							merge_len += 1
					# if id <= 2639999:
						# pass
					# else:
						# with open("./match_part_sen_no.sql", "a+", encoding='utf-8') as f:
							# if current == rowcount:
								# f.write("({0},{1},{2},'{3}','{4}',{5},{6},{7});".format(id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len))
							# else:
								# f.write("({0},{1},{2},'{3}','{4}',{5},{6},{7}),\n".format(id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len))
					if current == rowcount:
						result_list.append((id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len))
						# print("({0},{1},{2},'{3}','{4}',{5},{6},{7});".format(id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len))
					else:
						result_list.append((id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len))
						# print("({0},{1},{2},'{3}','{4}',{5},{6},{7});".format(id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len))
					id += 1
				# if not(current%10000):
					# print("({}/{})-- update OK".format(current,rowcount))

					

		t1 = datetime.now()
		print("讀取資料庫時間:{} sec".format((t1-t0).seconds))
		print("準備寫入資料庫，result_list長度:{}".format(len(result_list)))
		input(">>>")
		index = 0
		for r in result_list:
			# print(r)
			index += 1
			id,sen_idA,sen_idB,match_part,merge,score,match_len,merge_len = r
			sqlstr = """
					Insert into match_part (id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len)
					VALUES({},{},{},'{}','{}',{},{},{});
					"""
			sqlstr = sqlstr.format(id,sen_idA, sen_idB,match_part,merge,score,match_len,merge_len)
			# print(sqlstr)
			# input(">>>")
			cursor.execute(sqlstr)
			if not(index%10000):
				print("({}/{})-- update OK".format(index,len(result_list)))
		t2 = datetime.now()
		print("寫入資料庫時間:{} sec".format((t2-t1).seconds))
		
			
	finally:  
		# 要記得關 cursor、connection, 
		# 不然用 SSCursor 時會出現 warning
		#cursor.close()
		conn.commit()
		conn.close()
		
		
