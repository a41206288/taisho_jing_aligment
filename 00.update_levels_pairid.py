import csv
levelRow = []
with open('source_data/text_reuse_Levels.tsv', newline='',encoding = "utf-8") as c2:
	rows = csv.DictReader(c2, delimiter='\t')
	for index, rows2 in enumerate(rows):
		levelRow.append(rows2)

print("text_reuse_Levels讀取完成")
# exit()
with open('source_data/text_reuse_Levels_over20.tsv', 'w', newline='',encoding = "utf-8") as csvfile:
	# 建立 CSV 檔寫入器
	writer = csv.writer(csvfile,delimiter='	')						

	# 寫入一列資料
	writer.writerow(['pair_count','work1','sid1','work2','sid2','align_text1','align_text2','bestcLv','bestjLv'])

now_index = 0
result_list = []
with open('source_data/text_reuse_cluster.tsv', newline='',encoding = "utf-8") as c1:

	# 讀取 CSV 檔內容，將每一列轉成一個 dictionary
	rows = csv.DictReader(c1, delimiter='\t')
	# 以迴圈輸出指定欄位
	
	

	for row in rows:
		# print("現在row:{},{},{},{},{}".format(row['pair_count'],row['work1'],row['sid1'],row['work2'],row['sid2']))
			
		# for levelRow in levelDict[now_index:len(levelDict)]:
		# for levelRow in levelDict:
		
		# print(levelRow)
		for i in range(now_index,len(levelRow)):
			# print("現在index:{},{},{},{},{}".format(i,levelRow[i]['work1'],levelRow[i]['sid1'],levelRow[i]['work2'],levelRow[i]['sid2']))
			if row['work1']==levelRow[i]['work1'] and row['sid1']==levelRow[i]['sid1'] and row['work2']==levelRow[i]['work2'] and row['sid2']==levelRow[i]['sid2']:
				# print("比對成功")
				# print([levelRow['pair_count'],row['work1'],row['sid1'],row['work2'],row['sid2'],row['align_text1'],row['align_text2'],row['bestcLv'],row['bestjLv']])

				# result_list.append([levelRow[i]['pair_count'],row['work1'],row['sid1'],row['work2'],row['sid2'],row['align_text1'],row['align_text2'],row['bestcLv'],row['bestjLv']])
				
				with open('source_data/text_reuse_Levels_over20.tsv', 'a+',encoding='utf-8', newline='') as c3:
					writer = csv.writer(c3, delimiter='	')						
					writer.writerow([levelRow[i]['pair_count'],row['work1'],row['sid1'],row['work2'],row['sid2'],row['align_text1'],row['align_text2'],row['bestcLv'],row['bestjLv']])
				
				now_index = i+1
				# levelDict.pop(now_index)
				if not(now_index%10000):
					print("({}/{})-- output OK".format(now_index,len(levelRow)))
				break
			
			# else:
				# levelDict.pop(now_index)
			# print(rows2[0])
			# for row2 in rows2:
				# if row1['pair_count'] == row2['pair_count']:
					# now_id = row2['pair_count']
# print("開始寫入")
# index = 0
# for r in result_list:
	# with open('source_data/text_reuse_Levels_over20.tsv', 'a+',encoding='utf-8', newline='') as csvfile:
		# writer = csv.writer(csvfile)						
		# writer.writerow([r])
		# if not(now_index%10000):
			# print("({}/{})-- output OK".format(index,len(result_list)))
			# index +=1
