#計算所有sid的match_len
import mysql.connector

conn = mysql.connector.connect(user='root', passwd='',host='localhost', db="cbeta_text_reuse_over20")
			 
#cursor = conn.cursor(cursorclass=SSCursor)
cursor = conn.cursor()
cursor.execute("SET NAMES utf8mb4")
cursor.execute("set character_set_server = utf8mb4")
cursor.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令


sql = '''select count(*) 
FROM pair_sentence AS psa 
	join pair as pa on psa.pair_id = pa.id'''
cursor.execute(sql)
row = cursor.fetchone()
rowcount=row[0]
print ("共{}筆".format(rowcount))
#limit 3500000
# input(">>>")
sql = '''select sent_id,match_len
FROM pair_sentence AS psa 
	join pair as pa on psa.pair_id = pa.id
	'''
cursor.execute(sql)


# t0 = datetime.now()
			# delta = t2 - t1

# reuse_dict = {}
match_len_dic = {}
match_len = 0
try:
	current=0
	while True:
		rows = cursor.fetchmany(size=10000)
		print("({}/{})--OK".format(current,rowcount))
		if not rows:
			break
		# process your rows here.
		for row in rows:
				current+=1
				#讀取資料庫欄位
				sent_id,match_len = row
				if sent_id not in match_len_dic:
					match_len_dic[sent_id] = match_len
				elif match_len_dic[sent_id] < match_len:
					match_len_dic[sent_id] = match_len
finally:  
	# 要記得關 cursor、connection, 
	# 不然用 SSCursor 時會出現 warning
	#cursor.close()
	conn.commit()
	conn.close()

fw = open("out/sid_count.txt",'w+')
fw.write(str(match_len_dic))      #把字典转化为str
fw.close()

