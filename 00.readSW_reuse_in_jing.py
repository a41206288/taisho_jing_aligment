import os, csv
import struct
from pathlib import Path
import re
import mysql.connector

PWD= os.path.dirname(__file__)
SOURCE_BASE = "source_data"
SW_DATA_DIR = "sw2_gt20"
INGORE_PATTERN = r'([A-Za-z0-9□])'

def read_sw_csv(dirpath_to_csv,match_len_dic):
	
	"""
		傳入：dirpath_to_csv = csv 資料夾
	"""
	print("開始讀取句比對結果，..檔案資料夾：{}".format(dirpath_to_csv))
	

	#找出 SW_DATA_DIR 資料夾底下所有 .csv的檔案

	list_of_sw_csv =[]
	for (dirpath, dirnames, filenames) in os.walk(dirpath_to_csv):
		list_of_sw_csv += [os.path.join(dirpath_to_csv, file) for file in filenames if file[-3:]=="csv"]

	num_files = len(list_of_sw_csv) #待處理檔案數
	
	#讀取 比對結果 CSV
	# pair_count=0
	result_list = []

	for i,file in enumerate(list_of_sw_csv):
		print ("\r讀取 csv files: {:.2f} % ".format(i/num_files*100),end="")
		work1 = file.split("\\")[-1].split(".")[0]
		# work1 = file
		#紀錄與work1有關的所有經的相關總句數
		sid1_set = set()
		sid2_set = set()

		now_work2 = ""
		with open (file,"r",encoding='utf-8') as csvfile:
			rows = csv.reader(csvfile)

			next(rows)
			
			for work2, sid1, sid2, score, align1, align2, text1, text2, begin1,end1,begin2,end2 in rows:
				# 若align 內容有 數字, 英文字，全部都剔除
				if re.search(INGORE_PATTERN, align1) or re.search(INGORE_PATTERN, align2):
					continue
				#如果是第一筆
				if not now_work2:
					now_work2 = work2
					# print("讀取第一筆資料，now_work2:{}".format(now_work2))
					sid1_set.add(sid1)
					sid2_set.add(sid2)
					# print("sid1_set:{}".format(sid1_set))
					# print("sid2_set:{}".format(sid2_set))
				#如果換經 就紀錄work1 work2的重用字數與句數 並清空紀錄句號 進行下一經的統計
				elif work2 != now_work2:
					work1_word_len = 0
					work2_word_len = 0
					# 紀錄work1 work2的重用字數
					for s1 in sid1_set:
						try:
							work1_word_len += match_len_dic[int(s1)]
						except:
							print("align1:{}".format(align1))
							print("text1:{}".format(text1))
							# input(">>>")
					for s2 in sid2_set:
						try:
							work2_word_len += match_len_dic[int(s2)]
						except:
							print("align2:{}".format(align2))
							print("text2:{}".format(text2))
							# input(">>>")
					# 紀錄work1 work2的重用句數
					work1_sent_count = len(sid1_set)
					work2_sent_count = len(sid2_set)
					# 輸出到總結果result_list
					# print("換經，輸出比對結果:{}".format((work1,work1_sent_count,work1_word_len,now_work2,work2_sent_count,work2_word_len)))
					result_list.append((work1,work1_sent_count,work1_word_len,now_work2,work2_sent_count,work2_word_len))
					
					# 清空紀錄句號 記錄新經內容
					now_work2 = work2
					sid1_set.clear()
					sid2_set.clear()
					sid1_set.add(sid1)
					sid2_set.add(sid2)
					# print("清空sid1_set,sid2_set,now_work2:{}".format(now_work2))
					# input(">>>")
					# print("sid1_set:{}".format(sid1_set))
					# print("sid2_set:{}".format(sid2_set))
				#連續的經號
				else:
					sid1_set.add(sid1)
					sid2_set.add(sid2)
					# print("連續的經號")
					# print("sid1_set:{}".format(sid1_set))
					# print("sid2_set:{}".format(sid2_set))
				# print ("經號,句號: (work1:{},sid1:{},{}-{}), 經號,句號: (work2:{},sid2:{},{}-{})".format(
							# work1, sid1, begin1,end1, work2, sid2, begin2,end2))
				# print()
				# print("原句1:{} ...".format(text1[:40]))
				# print("原句1:{} ...".format(text2[:40]))
				# print("重用1:{}".format(align1[:40]))
				# print("重用2:{}".format(align2[:40]))
				# print("-"*40)
				# print(result_list)
	return result_list


# here start main function:

#讀取sid字數dic

print("讀取sid_count.txt")
with open("out/sid_count.txt","r+") as f:
	match_len_dic = eval(f.read())

#讀取 sw result csv, 並計算數量
sw_dirpath = os.path.join(PWD,SOURCE_BASE,SW_DATA_DIR)
result_list = read_sw_csv(sw_dirpath,match_len_dic)


conn = mysql.connector.connect(user='root', passwd='',host='localhost', db="cbeta_text_reuse_over20")
			 
#cursor = conn.cursor(cursorclass=SSCursor)
cursor = conn.cursor()
cursor.execute("SET NAMES utf8mb4")
cursor.execute("set character_set_server = utf8mb4")
cursor.execute("set character_set_database = utf8mb4")  #利用 cursor.execute 執行指令

index = 0
for r in result_list:
	index += 1
	work1,work1_sent_count,work1_word_len,now_work2,work2_sent_count,work2_word_len = r
	sqlstr = """
			Insert into text_reuse_in_jings (id,jing_idA,word_lenA,jing_idB,word_lenB)
			VALUES({},'{}',{},'{}',{});
			"""
	sqlstr = sqlstr.format(index-1,work1,work1_word_len,now_work2,work2_word_len)
	# print(sqlstr)
	# input(">>>")
	cursor.execute(sqlstr)
	if not(index%10000):
		print("({}/{})-- update OK".format(index,len(result_list)))
