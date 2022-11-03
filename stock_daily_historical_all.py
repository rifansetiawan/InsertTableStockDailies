import pandas as pd
import uuid
import mysql.connector
from datetime import datetime
import datetime
import time
from datetime import datetime,timedelta
import shutil
import csv

mydb = mysql.connector.connect(
  host="localhost",
  user="rifan",
  password="1234",
  database="pasardanamobile"
)

print(mydb)

mycursor = mydb.cursor()
# f = open('stock_daily_today.json')

while True:
    try:
        src = 'H:\\.shortcut-targets-by-id\\1Xa97Cqc118zC8pDRVJvCEhHGF27dyQ1f\\Intra5minutes\\intra5minutes Nov2022.csv'
        # src = 'I:\\.shortcut-targets-by-id\\1Xa97Cqc118zC8pDRVJvCEhHGF27dyQ1f\\Intra5minutes\\datasync5min-today.csv'
        dst = 'C:\\InsertTable\\InsertTableStockDailies\\intra5minutes_historical-copy.csv'
        shutil.copyfile(src, dst)

        csvFilePath = 'C:\saham\datasync5min-today-copy.csv'
        jsonFilePath = 'C:\saham\saham5minutesync.json'
        break
    except Exception as e:
        print("something error : " , e)

# print(df_agg)


mycursor.execute("DELETE FROM stock_daily_historicals")
sql = "INSERT INTO stock_daily_historicals (datetime,code,open,high,low,last,volume) VALUES"
val = "("

with open(dst) as f:
    cf = csv.DictReader(f)
    for i in cf:
        dateeeTime = datetime.strptime(i['Date/Time'], "%m/%d/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
        # rrrrr = dateeeTime.strftime("%Y-%m-%d %H:%M:%S")
        # dateeeTime = dateeeTime
        print(dateeeTime)
        # time.sleep(10000)
        data = "'"+str(dateeeTime).replace("T", " ") + "'" + "," + "'" + i['Ticker'] + "'" + ","  +str(i['Open'])  + "," + str(i['High']) + ","+ str(i['Low'])  + "," + str(i['Close']) + ","+ str( i['Volume'])
        command_val = "(" + data + ")" + ";"
        print(data)
        print(sql + command_val)
        
        mycursor.execute(sql + command_val)


mydb.commit()