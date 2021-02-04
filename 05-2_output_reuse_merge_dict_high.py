#重算資料庫text_reuse_in_fascicles資料表的資料，
#另外還會紀錄兩卷(fas)之前參考的pair，與其match的句號(sentence_id) 存入reuse_dict_test.txt

#import MySQLdb
from datetime import datetime
import mysql.connector
#from MySQLdb.cursors import SSCursor
# import json
# import pickle
from sklearn.externals import joblib

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
	FROM pair_sentence AS psa 
        join sentence AS sa on sa.id= psa.sent_id'''
	cursor.execute(sql)
	row = cursor.fetchone()
	rowcount=row[0]
	print ("共{}筆".format(rowcount))
	#limit 3500000
	# input(">>>")
	sql = '''select psa.id,sent_id,fas_id,text
	FROM pair_sentence AS psa 
        join sentence AS sa on sa.id= psa.sent_id'''
	cursor.execute(sql)
	
	
	t0 = datetime.now()
				# delta = t2 - t1
	
	reuse_dict = {}
	merge_pair_sen_dict = {}
	try:
		current=0
		while True:
			rows = cursor.fetchmany(size=10000)
			print("({}/{})--OK".format(current,rowcount))
			if not rows:
				break
			# process your rows here.
			Asent_id = ""
			Afas_id = ""
			for row in rows:
				current+=1
				#讀取資料庫欄位
				pair_sentence_id,sent_id,fas_id,text = row

				# pair_sentence中每個pair會存兩筆A和B，取id為偶數時判斷為第一筆，奇數判斷為第二筆
				if (int(pair_sentence_id) % 2) == 0:
					Asent_id = sent_id
					Afas_id = fas_id
					Atext = text
				else:
					Bsent_id = sent_id
					Bfas_id = fas_id
					Btext = text
					#取得左右兩句的流水卷號設為key 例(0,1)"長阿含經"對"七佛經"，value為對到的句子 例(13,4276) "如是我聞一時佛在舍衛國祇樹" 這句話為左句長阿含經的13句，同時也為右句七佛經的第4276句
					key = (Afas_id,Bfas_id,)
					value = (Asent_id,Bsent_id,)
					# 將結果存入reuse_dict
					if key not in reuse_dict:
						reuse_dict[key] = list()
					reuse_dict[key].append(value)
					# print(reuse_dict)
					# input(">>>")
					merge_pair_sen_dict[(Asent_id,Bsent_id,)] = (Atext,Btext,)
					
		#紀錄兩卷(fas)之前參考的pair，與其match的句號(sentence_id)
		'''
		fw = open("reuse_dict.txt",'w+')
		fw.write(str(reuse_dict))      #把字典转化为str
		fw.close()
		exit()
		=================================================
		with open('reuse_dict_test.pickle', 'wb') as f:
			pickle.dump(reuse_dict, f, 0)
		'''
		t1 = datetime.now()
		print("建立dic時間:{} sec".format((t1-t0).seconds))
		
		
		joblib.dump(reuse_dict,'out/reuse_dict_high.joblib')
		t2 = datetime.now()
		print("輸出reuse_dict時間:{} sec".format((t2-t1).seconds))
		
		
		joblib.dump(merge_pair_sen_dict, 'out/merge_pair_sen_dict_high.joblib')  
		t3 = datetime.now()
		print("輸出merge_pair_sen_dict時間:{} sec".format((t3-t2).seconds))
		
		# end of while-True
		
			
	finally:  
		# 要記得關 cursor、connection, 
		# 不然用 SSCursor 時會出現 warning
		#cursor.close()
		conn.commit()
		conn.close()
		
		
