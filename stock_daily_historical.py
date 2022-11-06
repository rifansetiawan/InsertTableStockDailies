import pandas as pd
import uuid
import mysql.connector
from datetime import datetime
import datetime
import time
from datetime import datetime,timedelta
import shutil
import csv
from datetime import date


now = datetime.now()
month_now = now.strftime("%b%Y")
date_now = now.strftime("%m/%#d/%Y")
print(date_now)
print(month_now)

header_temp = ['Ticker','Date/Time','Open','High','Low','Close','Volume','OpenInt','OpenInt']
data_temp = []

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
        src = 'H:\\.shortcut-targets-by-id\\1Xa97Cqc118zC8pDRVJvCEhHGF27dyQ1f\\Intra5minutes\\intra5minutes ' + month_now + '.csv'
        # src = 'I:\\.shortcut-targets-by-id\\1Xa97Cqc118zC8pDRVJvCEhHGF27dyQ1f\\Intra5minutes\\datasync5min-today.csv'
        dst = 'C:\\InsertTable\\InsertTableStockDailies\\intra5minutes_' + month_now + '-copy.csv'
        shutil.copyfile(src, dst)

        csvFilePath = 'C:\saham\datasync5min-today-copy.csv'
        jsonFilePath = 'C:\saham\saham5minutesync.json'
        break
    except Exception as e:
        print("something error : " , e)

with open(dst) as f:
    cf = csv.reader(f)
    next(cf)
    for row in cf:
        print(row)
        print(row[1][0:-9])
        if row[1][0:-9] != date_now:
            data_temp.append(row)
        
with open('all_historical_latest.csv', 'w', encoding='UTF8', newline='') as f_tmp:
    writer = csv.writer(f_tmp)
    writer.writerow(header_temp)
    writer.writerows(data_temp)

# print(df_agg)
df = pd.read_csv("all_historical_latest.csv")

df_agg=df.groupby("Ticker").last()


# mycursor.execute("DELETE FROM stock_dailies")
sql = "INSERT INTO stock_daily_historicals (datetime,code,open,high,low,last,volume) VALUES"
val = "("
for i in df_agg.itertuples():
    print(type(i[df.columns.get_loc('Date/Time')]))
    datetime_time = datetime.strptime(i[df.columns.get_loc('Date/Time')], '%m/%#d/%Y 00:00:00')
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