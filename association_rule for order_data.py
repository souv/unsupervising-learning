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
#6.save results:data,transactions,transactions_df....

import psycopg2
import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, fpmax, fpgrowth
import pickle as pkl

#連線至Postgresql
host = "kettle-pgdb-cluster.cluster-cxzihi7cb3wi.ap-northeast-1.rds.amazonaws.com"
dbname = "analyzedb"
user = "kettle"
password = "kettlepassword"

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

#filter date interval
data['main_order_day2'] = pd.to_datetime(data['main_order_day'], format='%Y-%m-%d')

data_0315 = data[(data.main_order_day2 >= '20210315') & (data.main_order_day2 <= '20210415')]

#filter gift
data_0315_2 =data_0315[~((data_0315['new_cate'] == '99_其他') | (data_0315['new_cate'] == '98_運費'))]

transactions = [a[1]['new_cate'].tolist() 
    for a in list(data_0315_2.groupby(['main_order_id']))]

te = TransactionEncoder()
te_ary = te.fit(transactions).transform(transactions)

#transactions會變成列是顧客，行類別的二維矩陣
transactions_df = pd.DataFrame(te_ary, columns=te.columns_)

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

res_cate.to_csv('訂單產品類別關聯分析0315-0415.csv',encoding = 'utf_8_sig')


#output pkl file
filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data.pkl'
pkl.dump(data,open(filename,'wb'))
filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions.pkl'
pkl.dump(transactions,open(filename,'wb'))
filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions_df.pkl'
pkl.dump(transactions_df,open(filename,'wb'))
filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_frequent_itemsets_cate.pkl'
pkl.dump(frequent_itemsets_cate,open(filename,'wb'))
filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data_final_freqsets.pkl'
pkl.dump(res_cate,open(filename,'wb'))

#load pkl file
filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data.pkl'
data = open(filename,'rb')
data = pkl.load(data)

filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions.pkl'
transactions = open(filename,'rb')
transactions = pkl.load(transactions)

filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_transactions_df.pkl'
transactions_df = open(filename,'rb')
transactions_df = pkl.load(transactions_df)

filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_frequent_itemsets_cate.pkl'
frequent_itemsets_cate = open(filename,'rb')
frequnet_itemsets_cate = pkl.load(frequent_itemsets_cate)

filename = 'C:\\Users\\lucaschang\\Desktop\\association rule\\訂單產品類別關聯分析0315-0415\\association_rule_data_final_freqsets.pkl'
final_freqsets = open(filename,'rb')
final_freqsets = pkl.load(final_freqsets)
