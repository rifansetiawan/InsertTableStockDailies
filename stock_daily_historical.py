import pandas as pd
import uuid
import mysql.connector
from datetime import datetime
import datetime
import time
from datetime import datetime,timedelta
import shutil


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
        dst = 'C:\\InsertTable\\InsertTableStockDailies\\intra5minutes_Oct2022-copy.csv'
        shutil.copyfile(src, dst)

        csvFilePath = 'C:\saham\datasync5min-today-copy.csv'
        jsonFilePath = 'C:\saham\saham5minutesync.json'
        break
    except Exception as e:
        print("something error : " , e)

# print(df_agg)
df = pd.read_csv(dst)

df_agg=df.groupby("Ticker").last()

date_stock_yesterday = '2022-10-21 00:00:00'

# mycursor.execute("DELETE FROM stock_dailies")

sql = "INSERT INTO stock_daily_historicals (datetime,code,open,high,low,last,volume) VALUES"
val = "("
for i in df_agg.itertuples():
    dateeeTime = datetime.strptime(i[df.columns.get_loc('Date/Time')], "%m/%d/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
    # rrrrr = dateeeTime.strftime("%Y-%m-%d %H:%M:%S")
    # dateeeTime = dateeeTime
    print(dateeeTime)
    # time.sleep(10000)
    data = "'"+str(dateeeTime).replace("T", " ") + "'" + "," + "'" + i.Index + "'" + ","  +str(i.Open)  + "," + str(i.High) + ","+ str(i.Low)  + "," + str(i.Close) + ","+ str( i.Volume)
    command_val = "(" + data + ")" + ";"
    print(data)
    print(sql + command_val)
    
    mycursor.execute(sql + command_val)
    # mydb.commit()
# mycursor.execute("update stock_dailies set diff = last - prev")
# mycursor.execute("update stock_dailies set diff_percentage = (last - prev) / prev")

mydb.commit()