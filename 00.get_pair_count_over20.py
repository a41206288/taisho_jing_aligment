import csv
import pickle 
#讀取

pair_count_over20 = []

with open('source_data/text_reuse_Levels_over20.tsv', newline='',encoding = "utf-8") as csvfile:

	# 讀取 CSV 檔內容，將每一列轉成一個 dictionary
	rows = csv.DictReader(csvfile, delimiter='\t')
	# 以迴圈輸出指定欄位
	for row in rows:
		
		pair_count_over20.append(row['pair_count'])

with open('out/pair_count_over20.pickle', 'wb') as f:
	pickle.dump(pair_count_over20, f, 0)