from datetime import datetime
import mysql.connector
import csv

if __name__ == '__main__':
	# conn = MySQLdb.connect(host='localhost',
						   # db='taisho_juan_aligment',
						   # user='root',
						   # passwd='',
						   # charset='utf8')
						   
	conn = mysql.connector.connect(user='root', passwd='',host='localhost', db="cbeta_text_reuse_clean_high")
			 
	#cursor = conn.cursor(cursorclass=SSCursor)
	cursor = conn.cursor()
	cursor.execute("SET NAMES utf8mb4")
	cursor.execute("set character_set_server = utf8mb4")
	cursor.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令
	
	cursorUpdate = conn.cursor()
	cursorUpdate.execute("SET NAMES utf8mb4")
	cursorUpdate.execute("set character_set_server = utf8mb4")
	cursorUpdate.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令

	count = 0
	with open('04.C_all-J_all.tsv', newline='',encoding = "utf-8") as csvfile:

		# 讀取 CSV 檔內容，將每一列轉成一個 dictionary
		rows = csv.DictReader(csvfile, delimiter='\t')
		# 以迴圈輸出指定欄位
		for row in rows:
			
			if row['bestcLv']=="high" and row['bestcLv']=="high":
				count+=1
	print("high/high共:{}筆".format(count))
	
	t0 = datetime.now()
				# delta = t2 - t1
	index = 0
	#寫入資料庫
	
	with open('04.C_all-J_all.tsv', newline='',encoding = "utf-8") as csvfile:

			# 讀取 CSV 檔內容，將每一列轉成一個 dictionary
			rows = csv.DictReader(csvfile, delimiter='\t')
			# 以迴圈輸出指定欄位
			for row in rows:
				
				if row['bestcLv']=="high" and row['bestcLv']=="high":
					index+=1
					# print(row['pair_count'],row['sid1'],row['sid2'])
					# input(">>>")
					sqlstr = """
								DELETE FROM sentence WHERE id = '{}';
							"""
					sqlstr = sqlstr.format(row['sid1'])
					# print(sqlstr)		
					cursor.execute(sqlstr)
					sqlstr = """
								DELETE FROM sentence WHERE id = '{}';
							"""
					sqlstr = sqlstr.format(row['sid2'])
					# print(sqlstr)		
					cursor.execute(sqlstr)
					
					if not(index%10000):
						print("({}/{})-- update OK".format(index,count))



	t1 = datetime.now() # end of while-True
	print("刪除資料時間:{} sec".format((t1-t0).seconds))
	conn.commit()
	conn.close()




