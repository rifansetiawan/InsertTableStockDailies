import pandas as pd
import uuid
import mysql.connector
from datetime import datetime
import datetime
import time
from datetime import datetime,timedelta
df = pd.read_csv('daily.csv')

df_agg=df.groupby("Code").last()

date_stock_yesterday = '2022-10-17 00:00:00'

mydb = mysql.connector.connect(
  host="localhost",
  user="rifan",
  password="1234",
  database="pasardanamobile"
)

print(mydb)

mycursor = mydb.cursor()
f = open('stock_daily_today.json')


# print(df_agg)

sql = "INSERT INTO stock_dailies (datetime,code,open,high,low,last,volume, prev, stock_logo,name, uuid) VALUES"
val = "("
for i in df_agg.itertuples():
    dateeeTime = datetime.strptime(i.DateTime, "%m/%d/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
    # rrrrr = dateeeTime.strftime("%Y-%m-%d %H:%M:%S")
    # dateeeTime = dateeeTime
    print(dateeeTime)
    # time.sleep(10000)
    stock_data_yesterday = "(select close from stock_datas where stock_code = '" + i.Index +  "'" + "and date = '" + date_stock_yesterday + "')"
    stock_id = "(select uuid from stocks where code = '" + i.Index + "')"
    stock_data_yesterday_diff = "(" + str(i.Last) + " - (select close from stock_datas where stock_code = '" + i.Index +  "'" + "and date = '" + date_stock_yesterday + "')"
    stock_logo = "( select filename from stock_logos where stock_code = '" + i.Index  + "')"
    stock_name = "( select name from stocks where code = '"  + i.Index  + "')"
    data = "'"+str(dateeeTime).replace("T", " ") + "'" + "," + "'" + i.Index + "'" + ","  +str(i.Open)  + "," + str(i.High) + ","+ str(i.Low)  + "," + str(i.Last) + ","+ str( i.Volume) + "," + stock_data_yesterday + ","+stock_logo + ","+ stock_name + ",'" + str(uuid.uuid4())+ "'"
    command_val = "(" + data + ")" + ";"
    print(data)
    print(sql + command_val)
    
    mycursor.execute(sql + command_val)
    mydb.commit()
mydb.commit()