# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 16:05:56 2021

@author: lucaschang
"""

#define data
#1.Time interval :　2021/03/15-2021/04/15
#2.sap_product_line relationship(中文線以MCH6來看)
#3.input data:order_data & mch_category data
#4.output data:rules, order prod & order sap_product_line
#5.排除掉sap_product_line = ''99_其他'' , ''98_運費''
#6.save results pickle:data,transactions,transactions_df....

# Import the os module
import os
import psycopg2
import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, fpmax, fpgrowth
import pickle as pkl

# Get the current working directory
cwd = os.getcwd()

# Change the current working directory
os.chdir('C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415')

###1.連線至Postgresql###
host = ""
dbname = ""
user = ""
password = ""

conn_string = "host={0} user={1} dbname={2} password={3}".format(host, user, dbname, password)
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
cursor.execute("select *,(case when mch_3 = '111' then mch_description_2 else sap_product_line end) as new_cate  \
                        from ec2_order_item_detail_all a \
                         left join \
	                   (select * from public.ec2_mch_category) b \
                       on a.mch = b.mch;")


data = pd.DataFrame(cursor.fetchall())
column_names = [i[0] for i in cursor.description]
data.columns = column_names

###2.filter data###
#filter date interval
data['main_order_day2'] = pd.to_datetime(data['main_order_day'], format='%Y-%m-%d')

data_0315 = data[(data.main_order_day2 >= '20210315') & (data.main_order_day2 <= '20210415')]

#filter out 贈品及運費
data_0315_2 =data_0315[~((data_0315['new_cate'] == '99_其他') | (data_0315['new_cate'] == '98_運費'))]

###3.turn transactions record to transactions list###
transactions = [a[1]['new_cate'].tolist() 
    for a in list(data_0315_2.groupby(['main_order_id']))]

###4.calculate transaction contribution through order item amount ###
#計算訂單總金額
main_order_amount = data_0315_2.groupby('main_order_id')['real_item_amount'].sum()

#交易資料和訂單總金額併起來
test = pd.DataFrame(transactions)
test2 = pd.DataFrame(main_order_amount)
test3 = pd.concat([test.reset_index(),test2.reset_index()],axis = 1)

#testing if order data is correct
data_0315_2[data_0315_2['main_order_id'] == 118880]['sap_product_line']
data_0315_2[data_0315_2['main_order_id'] == 118880][['product_name','new_cate']]

#訂單類別及訂單總金額
test3.to_csv('類別組合貢獻＿訂單.csv',encoding = 'utf_8_sig')

#訂單類別，總金額及訂單購買項目資料
order_raw_data = test3.merge(data_0315_2,
                   how = 'left',left_on = 'main_order_id',
                   right_on = 'main_order_id')

order_raw_data.to_csv('類別組合貢獻＿原始購買資料.csv',encoding = 'utf_8_sig')

#前兩項產品類別購買金額(僅會計算訂單前兩項產品的貢獻)
cate_bind_rev = test3.groupby([0,1])['real_item_amount'].sum()
cate_bind_al itemrev.to_csv('類別組合貢獻_前兩項產品.csv',encoding = 'utf_8_sig')

###5.change transaction data into that can be doing asscoiation rule###
te = TransactionEncoder()
te_ary = te.fit(transactions).transform(transactions)

#transactions會變成列是顧客，行類別的二維矩陣
transactions_df = pd.DataFrame(te_ary, columns=te.columns_)

###6.association rule###
frequent_itemsets_cate = apriori(transactions_df, 
                                min_support=0.001,
                                use_colnames=True)

frequent_itemsets_cate['length'] = frequent_itemsets_cate['itemsets'].apply(lambda x: len(x))
len(frequent_itemsets_cate)

#
res_cate = association_rules(frequent_itemsets_cate,
                        metric="confidence",
                        min_threshold= 0)

len(res_cate.index)

final_freqsets['test_ante'] = tuple(final_freqsets['antecedents'])


res_cate.to_csv('訂單產品類別關聯分析0315-0415.csv',encoding = 'utf_8_sig')

###7.output generate pickle for future usage###
#output pkl file
filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data.pkl'
pkl.dump(data,open(filename,'wb'))

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions.pkl'
pkl.dump(transactions,open(filename,'wb'))

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions_df.pkl'
pkl.dump(transactions_df,open(filename,'wb'))

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_frequent_itemsets_cate.pkl'
pkl.dump(frequent_itemsets_cate,open(filename,'wb'))

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data_final_freqsets.pkl'
pkl.dump(res_cate,open(filename,'wb'))

#load pkl file
filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data.pkl'
data = open(filename,'rb')
data = pkl.load(data)

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions.pkl'
transactions = open(filename,'rb')
transactions = pkl.load(transactions)

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions_df.pkl'
transactions_df = open(filename,'rb')
transactions_df = pkl.load(transactions_df)

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_frequent_itemsets_cate.pkl'
frequent_itemsets_cate = open(filename,'rb')
frequnet_itemsets_cate = pkl.load(frequent_itemsets_cate)

filename = 'C:\\Users\\lucaschang\\Desktop\\modeling\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data_final_freqsets.pkl'
final_freqsets = open(filename,'rb')
final_freqsets = pkl.load(final_freqsets)
