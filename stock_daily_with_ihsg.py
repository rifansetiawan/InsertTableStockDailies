import pandas as pd
import uuid
import mysql.connector
from datetime import datetime
import datetime
import time
from datetime import datetime,timedelta
import shutil
from datetime import date


today = date.today()

today_str = str(today) + " 00:00:00"
today_str = "2023-01-13 00:00:00"


print("today str : ", today_str)

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
        src = 'H:\\.shortcut-targets-by-id\\1Xa97Cqc118zC8pDRVJvCEhHGF27dyQ1f\\Intra5minutes\\datasync5min-today.csv'
        # src = 'I:\\.shortcut-targets-by-id\\1Xa97Cqc118zC8pDRVJvCEhHGF27dyQ1f\\Intra5minutes\\datasync5min-today.csv'
        dst = 'C:\\InsertTable\\InsertTableStockDailies\\datasync5min-today-copy.csv'
        shutil.copyfile(src, dst)

        csvFilePath = 'C:\saham\datasync5min-today-copy.csv'
        jsonFilePath = 'C:\saham\saham5minutesync.json'
        break
    except Exception as e:
        print("something error : " , e)

# print(df_agg)
df = pd.read_csv(dst)

df_agg=df.groupby("Ticker").last()

date_stock_yesterday = '2022-10-26 00:00:00'

mycursor.execute("DELETE FROM stock_dailies")
sql = "INSERT INTO stock_dailies (datetime,code,open,high,low,last,volume, prev, stock_logo,name, uuid) VALUES"
val = "("
for i in df_agg.itertuples():
    dateeeTime = datetime.strptime(i[df.columns.get_loc('Date/Time')], "%m/%d/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
    # rrrrr = dateeeTime.strftime("%Y-%m-%d %H:%M:%S")
    # dateeeTime = dateeeTime
    print(dateeeTime)
    # time.sleep(10000)
    stock_data_yesterday = "(select last from stock_daily_historicals where code = '" + i.Index +  "'" + " ORDER BY DATETIME DESC LIMIT 1" + ")"
    stock_id = "(select uuid from stocks where code = '" + i.Index + "')"
    stock_data_yesterday_diff = "(" + str(i.Close) + " - (select close from stock_daily_historicals where code = '" + i.Index +  "'" + "ORDER BY DATE DESC LIMIT 1" + ")"
    stock_logo = "( select filename from stock_logos where stock_code = '" + i.Index  + "')"
    stock_name = "( select name from stocks where code = '"  + i.Index  + "')"
    stock_highest = "(select high from stock_daily_historicals where code = '" + i.Index + "' and datetime >= "+"'"+today_str+"'"+ " ORDER BY high desc limit 1)"
    stock_lowest = "(select low from stock_daily_historicals where code = '" + i.Index + "' and datetime >=  "+"'"+today_str +"'"+ " ORDER BY low asc limit 1)"
    stock_volume = "( select sum(volume) from stock_daily_historicals where code = '"  + i.Index  + "' and datetime >= '"+today_str+"' )"

    # stock_highest_df = df.groupby("Ticker")[""]
    # stock_lowest_df = df.groupby("Ticker")[""]
    stock_volume_df = df.groupby("Ticker")
    print(stock_volume_df)
    time.sleep(1000)
    print("stock highest : ", stock_highest)
    print("stock lowest : ", stock_lowest)
    print("stock volume : ", stock_volume)

    # time.sleep(1000)
    if i.Index == 'IDXBASIC':
        stock_name = "'Sektor Barang Baku'"
    elif i.Index == 'IDXCYCLIC':
        stock_name = "'Sektor Konsumer Non-Primer'"
    elif i.Index == 'IDXENERGY':
        stock_name = "'Sektor Energi'"
    elif i.Index == 'IDXFINANCE':
        stock_name = "'Sektor Keuangan'"
    elif i.Index == 'IDXHEALTH':
        stock_name = "'Sektor Kesehatan'"
    elif i.Index == 'IDXINDUST':
        stock_name = "'Sektor Perindustrian'"
    elif i.Index == 'IDXINFRA':
        stock_name = "'Sektor Infrastruktur'"
    elif i.Index == 'IDXNONCYC':
        stock_name = "'Sektor Konsumen Primer'"
    elif i.Index == 'IDXPROPERTY':
        stock_name = "'Sektor Properti dan Real Estat'"
    elif i.Index == 'IDXTECHNO':
        stock_name = "'Sektor Teknologi'"
    elif i.Index == 'IDXTRANS':
        stock_name = "'Sektor Transportasi dan Logistik'"
    elif i.Index == 'COMPOSITE':
        stock_name = "'IHSG'"

    data = "'"+str(dateeeTime).replace("T", " ") + "'" + "," + "'" + i.Index + "'" + ","  +str(i.Open)  + "," + stock_highest + ","+ stock_lowest  + "," + str(i.Close) + ","+ stock_volume + "," + stock_data_yesterday + ","+stock_logo + ","+ stock_name + ",'" + str(uuid.uuid4())+ "'"
    command_val = "(" + data + ")" + ";"
    print(data)
    print(sql + command_val)
        
    mycursor.execute(sql + command_val)
    
    # if i.Index == 'COMPOSITE' or i.Index("IDX", 0, 3) :
    #     stock_data_yesterday = "(select last from stock_daily_historicals where code = '" + i.Index +  "'" + "ORDER BY DATETIME DESC LIMIT 1" +  ")"
    #     stock_id = "(select uuid from stocks where code = '" + i.Index + "')"
    #     stock_data_yesterday_diff = "(" + str(i.Close) + " - (select close from stock_daily_historicals where code = '" + i.Index +  "'" +  "ORDER BY DATE DESC LIMIT 1" +  ")"
    #     stock_logo = "( select filename from stock_logos where stock_code = '" + i.Index  + "')"
    #     stock_name = "( select name from stocks where code = '"  + i.Index  + "')"
    #     data = "'"+str(dateeeTime).replace("T", " ") + "'" + "," + "'" + i.Index + "'" + ","  +str(i.Open)  + "," + str(i.High) + ","+ str(i.Low)  + "," + str(i.Close) + ","+ str( i.Volume) + "," + stock_data_yesterday + ","+stock_logo + ","+ stock_name + ",'" + str(uuid.uuid4())+ "'"
    #     command_val = "(" + data + ")" + ";"
    #     print(data)
    #     print(sql + command_val)
        
    #     mycursor.execute(sql + command_val)

    # else :
    #     stock_data_yesterday = "(select close from stock_datas where stock_code = '" + i.Index +  "'" +  "ORDER BY DATE DESC LIMIT 1" +  ")"
    #     stock_id = "(select uuid from stocks where code = '" + i.Index + "')"
    #     stock_data_yesterday_diff = "(" + str(i.Close) + " - (select close from stock_datas where stock_code = '" + i.Index +  "'" + "ORDER BY DATE DESC LIMIT 1" +  ")"
    #     stock_logo = "( select filename from stock_logos where stock_code = '" + i.Index  + "')"
    #     stock_name = "( select name from stocks where code = '"  + i.Index  + "')"
    #     data = "'"+str(dateeeTime).replace("T", " ") + "'" + "," + "'" + i.Index + "'" + ","  +str(i.Open)  + "," + str(i.High) + ","+ str(i.Low)  + "," + str(i.Close) + ","+ str( i.Volume) + "," + stock_data_yesterday + ","+stock_logo + ","+ stock_name + ",'" + str(uuid.uuid4())+ "'"
    #     command_val = "(" + data + ")" + ";"
    #     print(data)
    #     print(sql + command_val)
    
    #     mycursor.execute(sql + command_val)
    # # mydb.commit()
mycursor.execute("update stock_dailies set diff = last - prev")
mycursor.execute("update stock_dailies set diff_percentage = (last - prev) / prev")

mydb.commit()