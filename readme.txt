此資料夾根據20191106新版資料庫
將原本包含10字以上重複的部分清理為只剩20字以上的部分

source_data/sw2_gt20
記錄經跟經之間的所有重用pair，按照經號建立一個檔案

source_data/text_reuse_Levels.tsv
記錄所有包含10字以上重複的部分pair的high low關係
此檔案的pair_id 是正確的，用來更正pair_id

source_data/text_reuse_Levels_over20.tsv
記錄所有包含20字以上重複的部分pair的high low關係

00.get_pair_count_over20.py
read
source_data/text_reuse_Levels_over20.tsv
goal
取出over20的所有pair_id
out
out/pair_count_over20.pickle


02-3_del_pair.py
read
db cbeta_text_reuse_over20/pair (內容為資料庫cbeta_text_reuse的內容)
out/pair_count_over20.pickle
goal
清除所有低於20字的pair

02-3_del_pairsen.py
read
db cbeta_text_reuse_over20/pair_sentence 內容為資料庫cbeta_text_reuse的內容
out/pair_count_over20.pickle
goal
清除所有低於20字的pair

00.sid_count.py
read
db cbeta_text_reuse_over20/pair_sentence
db cbeta_text_reuse_over20/pair
goal
計算所有sid的match_len，產生sid:match_len的dic
out
out/sid_count.txt

00.readSW_reuse_in_jing
read
source_data/sw2_gt20
out/sid_count.txt
goal
輸出每兩部經之間的的重用字數
out
db cbeta_text_reuse_over20/text_reuse_in_jings



