# 輸入A經 : T0220 列出與T0220有關的所有經與重複的內容
# 又輸入B經 :列出AB經的相關內容
from datetime import datetime
import mysql.connector
#from MySQLdb.cursors import SSCursor
# import json
# import pickle
from sklearn.externals import joblib
import csv

# 判斷比對經號是否小於指定年代
def checkYear (jing,inputYear):
	with open('source_data/taisho-concise-catalog-2017-05-31.csv', newline='',encoding = "utf-8") as csvfile:

		# 讀取 CSV 檔內容，將每一列轉成一個 dictionary
		rows = csv.DictReader(csvfile)
		# 以迴圈輸出指定欄位
		for row in rows:
			if row['大正藏經號'] == jing and row['大正藏1起始年']!= "" and int(row['大正藏1起始年']) <= int(inputYear):
				# print("經號符合年代。{},{}".format(row['大正藏經號'], row['大正藏1起始年']))
				return True 

if __name__ == '__main__':

	t0 = datetime.now()
	print("讀取reuse_dict.joblib")
	#讀取經文重用pair及其重用句號的dic
	'''
	with open("reuse_dict_test.pickle","rb") as f:
		reuse_dict = pickle.load(f)
	================================================
	with open("reuse_dict_test.pickle","rb") as f:
		for fLine in f:
			# print(fLine)
			reuse_dict = eval(fLine)
	================================================
	fr = open("reuse_dict_test.txt",'r+')
	for line in fr:
		print (line)
		reuse_dict = eval(line)   #读取的str转换为字典
	# print(reuse_dict)
	fr.close()
	'''
	reuse_dict = joblib.load('out/reuse_dict.joblib')
	# reuse_dict = joblib.load('reuse_dict_test.joblib')
	t1 = datetime.now()
	print("讀取reuse_dict完成:{} sec".format((t1-t0).seconds))
	# print(reuse_dict)
	# input(">>>")
	
	#讀取重用句號與重用文字的dic
	print("讀取merge_pair_sen_dic.joblib")
	merge_pair_sen_dict = joblib.load('out/merge_pair_sen_dict.joblib')
	# merge_pair_sen_dict = joblib.load('merge_pair_sen_dict_test.joblib')
	t2 = datetime.now()
	print("讀取merge_pair_sen_dict完成:{} sec".format((t2-t1).seconds))
	# print(merge_pair_sen_dict)
	# input(">>>")
		
		
	print("讀取jing_fascicle.txt")
	#讀取經號的相關卷數
	t3 = datetime.now()
	fr = open("out/jing_fascicle.txt",'r+')
	jing_fas_dict = eval(fr.read())   #读取的str转换为字典
	# print(jing_fas_dict)
	fr.close()
	print("讀取jing_fascicle完成:{} sec".format((t3-t2).seconds))
	# input(">>>")
	
	
	while True:
		while True:
			inputA = input("請輸入比較經號A(格式:T0001):")
			#取得起始、結尾經號在dirArray中的編號(index)
			if inputA not in jing_fas_dict:
				print("輸入經號A不存在，請重新輸入。\n")
			else:
				inputB = input("請輸入比較經號B(格式:T0001, 可空白):")
				#有輸入B
				if inputB:
					if inputB not in jing_fas_dict:
						print("輸入經號B不存在，請重新輸入。\n")
						continue
					if inputB == inputA:
						print("不做同經比較，請重新輸入。\n")
					else:
						break
				#未輸入B
				else:
					inputB = ""
					break
		
		#先取出輸入經號的相關卷數，再尋找相關連的句對
		fas_sen = {}
		#有輸入B
		if inputB:
			# print("經號{}的相關卷數:{}".format(inputA,jing_fas_dict[inputA]))
			# print("經號{}的相關卷數:{}".format(inputB,jing_fas_dict[inputB]))
			for key in reuse_dict.keys():
				# print("現在key:{}".format(key))
				#選出所有重用句數 判斷是否為A.B經的所有卷數組合
				if (key[0] in jing_fas_dict[inputA] and key[1] in jing_fas_dict[inputB]) or (key[1] in jing_fas_dict[inputA] and key[0] in jing_fas_dict[inputB]):
					# print("加入key:{}的:{}".format(key,reuse_dict[key]))
					fas_sen[key] = reuse_dict[key]
					# input(">>>")
			if fas_sen:
				# print("總相關句號:{}".format(fas_sen))
				print("輸出{},{}.csv。\n".format(inputA,inputB))
			else:
				print("不存在與{}的重用現象。".format(inputA))
				
			# input(">>>")
		#未輸入B
		else:
			inputYear = input("請輸入輸出結果年代區間小於A.D.(格式:705, 可空白):")
			# print("經號{}的相關卷數:{}".format(inputA,jing_fas_dict[inputA]))
			
			#判斷經A的每卷是否出現在重用字典裡，存在則取出句號(sen_id)
			for fasA in jing_fas_dict[inputA]:
				# print("現在卷號:{}".format(fasA))
				#現在卷號:0
				for key in reuse_dict.keys():
					# print("現在key:{}".format(key))
					#現在key:(0, 1)現在卷號0存在於(0,1)內，故加入其所有句對
					#(2, 1),(3, 1),(4, 1)...都沒有0 所以跳過
					
					# 2020.03.10 如果有輸入年代
					if inputYear:
						# 取出包含經A卷號的key對 ex.0在 key:(0, 1)裡
						if fasA in key:
							for fas in key:
								# 選擇與經A卷號有重用的卷號
								if fas != fasA:
									# print("現在key內選擇的卷號:{}".format(fas))
									# 取出他的經號
									for jing_key,jing_value in jing_fas_dict.items():
										if fas in jing_value:
											jing = jing_key
											# print("現在key內選擇的卷號對應的經號:{}".format(jing))
											# 判斷此經號是否符合年代
											if checkYear(jing,inputYear):
												fas_sen[key] = reuse_dict[key]
												# input(">>>")
											# else:
												# print("年代不符")
					# 沒輸入年代
					else:
						if fasA in key:
							# print("加入key:{}的句對:{}".format(key,reuse_dict[key]))
							# 加入key:(0, 1)的句對:[(13, 4276)]
							fas_sen[key] = reuse_dict[key]
							# input(">>>")
			if fas_sen:
				# print("總相關句號:{}".format(fas_sen))
				print("未輸入經號B，將輸出{}csv。\n".format(inputA))
			else:
				print("不存在與{}的重用現象。".format(inputA))

			# input(">>>")
		
		t4 = datetime.now()
		with open("out/reuse_high/{}_{}_reuse.csv".format(inputA,inputB), 'w', encoding='utf-8', newline='') as f:
			writer = csv.writer(f, delimiter=',')
			writer.writerow(['經號A','卷號A','句號A','原句A','經號B','卷號B','句號B','原句B']) #第⼀一⾏行行的標題
		
		index = 0
		dic_len = 0
		for fas_key,fas_value in fas_sen.items():
			dic_len += len(fas_value)
		print("共{}筆".format(dic_len))
		
		for fas_key,fas_value in fas_sen.items():
			# print("卷號:{}".format(fas_key))
			#由卷號找回經號
			fasA = fas_key[0]
			fasB = fas_key[1]
			for jing_key,jing_value in jing_fas_dict.items():
				if fasA in jing_value:
					jingA = jing_key
				if fasB in jing_value:
					jingB = jing_key
			# print("對應經號:{},{}".format(jingA,jingB))
			# print("fas_value:{}".format(fas_value))
			for sen_pair in fas_value:
				index += 1
				senA = sen_pair[0]
				senB = sen_pair[1]
				matchA = merge_pair_sen_dict[sen_pair][0]
				matchB = merge_pair_sen_dict[sen_pair][1]
				
				#待處理

					
				
				with open("out/reuse_high/{}_{}_reuse.csv".format(inputA,inputB), 'a+', encoding='utf-8',newline='') as f:
					writer = csv.writer(f, delimiter=',')
					if jingA != inputA:
						writer.writerow([ jingB,fasB,senB,matchB,jingA,fasA,senA,matchA ])
					else:
						writer.writerow([ jingA,fasA,senA,matchA,jingB,fasB,senB,matchB ])
				# print("sen_pair:{},match_part:{}".format(sen_pair,merge_pair_sen_dict[sen_pair]))
				# print(jingA,fasA,senA,matchA,jingB,fasB,senB,matchB)

				if not(index%1000):
					print("({}/{})-- output OK".format(index,dic_len))
		t5 = datetime.now()
		print("輸出csv完成:{} sec".format((t5-t4).seconds))