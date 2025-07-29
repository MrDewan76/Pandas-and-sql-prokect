#extract from zipfile
import zipfile
zip_ref = zipfile.ZipFile('orders.csv.zip')
zip_ref.extractall() #you can specify the path where you want the extract
zip_ref.close()


import pandas as pd
df = pd.read_csv('orders.csv', na_values= ['Not Available','unknown'])
print(df['Ship Mode'].unique())
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ','_')

#calculate discount.sale price and profit
df['discount'] = df['list_price']*df['discount_percent']*.01
df['sale_price'] = df['list_price'] - df['discount']
df['profit'] = df['sale_price'] - df['cost_price']

#change datatype of date column
df['order_date'] = pd.to_datetime(df['order_date'],format='%Y-%m-%d')
df['year'] = df['order_date'].dt.year
df['month'] = df['order_date'].dt.month
df['day'] = df['order_date'].dt.day

#drop cost_price,list_price and discount percent columns
df.drop(columns = ['list_price','cost_price','discount_percent'],inplace=True)

import sqlalchemy as sal
import duckdb as duck

#find top 10 highest revenue generating products
result1 = duck.query('Select product_id,Sum(sale_price) from df group by 1 order by 2 desc limit 10')
#(result1)

#find top 5 highest selling products in each region

result2 = duck.query('Select Region,product_id,Sales from(Select region,product_id,Sum(sale_price) as Sales , '
                     'row_number() over (partition by region order by Sum(sale_price) Desc) as rnk from df group by 1,2)'
                     ' where rnk <= 5 order by 1,3')
#print(result2)

#find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023
result3 = duck.query('Select month , sum(case when year = 2022 then sale_price else 0 end) as sales_2022'
                     ',sum(case when year = 2023 then sale_price else 0 end) as sales_2023 '
                     'from df '
                     'group by 1 order by 1')
print(result3)
print(df.columns)

