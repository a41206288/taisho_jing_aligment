import csv
import pickle 
#讀取

high_high_sid = []

with open('source_data/text_reuse_Levels_over20.tsv', newline='',encoding = "utf-8") as csvfile:

	# 讀取 CSV 檔內容，將每一列轉成一個 dictionary
	rows = csv.DictReader(csvfile, delimiter='\t')
	# 以迴圈輸出指定欄位
	for row in rows:
		
		if row['bestcLv']=="high" and row['bestcLv']=="high":
			# index+=1
			# print(row['pair_count'],row['sid1'],row['sid2'])
			# input(">>>")
			high_high_sid.append((row['sid1'],row['sid2']))

with open('out/high_high_sid.pickle', 'wb') as f:
	pickle.dump(high_high_sid, f, 0)