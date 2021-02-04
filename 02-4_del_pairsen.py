from datetime import datetime
import mysql.connector
import MySQLdb
from MySQLdb.cursors import SSCursor
import csv
import pickle

if __name__ == '__main__':
	conn = MySQLdb.connect(host='localhost',
						   db='cbeta_text_reuse_over20',
						   user='root',
						   passwd='',
						   charset='utf8')
						   
	# conn = mysql.connector.connect(user='root', passwd='',host='localhost', db="cbeta_text_reuse_over20")
			 
	cursor = conn.cursor(cursorclass=SSCursor)
	# cursor = conn.cursor(buffered=True)
	# cursor.execute("SET NAMES utf8mb4")
	# cursor.execute("set character_set_server = utf8mb4")
	# cursor.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令

	sql = '''select count(*) 
	FROM pair_sentence'''
	cursor.execute(sql)
	row = cursor.fetchone()
	rowcount=row[0]
	print ("共{}筆".format(rowcount))
	#limit 3500000
	# input(">>>")
	sql = '''select pair_id,sent_id
	FROM pair_sentence'''
	cursor.execute(sql)
	
	
	t0 = datetime.now()
				# delta = t2 - t1
	
	#讀取所有至少20字以上的pair_id 清理資料庫
	pair_count_over20 = []
	with open("out/pair_count_over20.pickle","rb") as f:
		pair_count_over20 = pickle.load(f)
		
	result_list = []
	
	index = 0
	try:
		current=0
		row1_pair_id = -1
		while True:
			rows = cursor.fetchmany(size=10000)
			# print("fetch10000.")
			if not rows:
				# print("not rows.")
				break
			# process your rows here.
			
			for row in rows:
				
				#讀取資料庫欄位
				pair_id,sent_id = row
				if pair_id == row1_pair_id:
					# print("row1_pair_id:{},同第二筆pair_id:{},sent_id:{}，跳過".format(row1_pair_id,pair_id,sent_id))
					continue
				elif pair_id>row1_pair_id:
					# print("row1_pair_id:{}<pair_id:{},sent_id:{}，覆蓋".format(row1_pair_id,pair_id,sent_id))
					row1_pair_id = pair_id

				while pair_id>int(pair_count_over20[index]):
					# print("原先資料庫已刪除，增加index")
					index+=1
					# print("current:{},index:{},資料庫pair_id:{},pair_id:{}".format(current,index,pair_id,pair_count_over20[index]))
				
				if pair_id != int(pair_count_over20[index]):
					sqlstr = """
								DELETE FROM pair_sentence WHERE pair_id = '{}';
							"""
					sqlstr = sqlstr.format(pair_id)
					result_list.append(sqlstr)
					# print(sqlstr)
					# print("不相等刪除")
					# print("current:{},index:{},資料庫pair_id:{},pair_id:{}".format(current,index,pair_id,pair_count_over20[index]))
					
				else:
					# print("相等")
					# print("current:{},index:{},資料庫pair_id:{},pair_id:{}".format(current,index,pair_id,pair_count_over20[index]))
					index+=1
				current+=1
				if not(current%10000):
					print("({}/{})-- read OK".format(current,rowcount))
		print("result_list筆數:{}，寫入資料庫".format(len(result_list)))
		input(">>>")
		r_count = 0
		for r in result_list:
			cursor.execute(r)
			r_count += 1
			if not(r_count%10000):
				print("({}/{})-- update OK".format(r_count,len(result_list)))
	
	
	finally:  
		# 要記得關 cursor、connection, 
		# 不然用 SSCursor 時會出現 warning
		#cursor.close()
		conn.commit()
		cursor.close()
	
	t1 = datetime.now() # end of while-True
	print("刪除資料時間:{} sec".format((t1-t0).seconds))




