import json
import mysql.connector
import time
import datetime
from datetime import datetime,timedelta

mydb = mysql.connector.connect(
  host="localhost",
  user="rifan",
  password="1234",
  database="pasardanamobile"
)

print(mydb)

mycursor = mydb.cursor()
f = open('stock_daily_today.json')

data = json.load(f)
# yesterday = data[0]["DateTime"].replace("T", " ")
# yesterday_time = datetime.strptime(yesterday, '%Y-%m-%d %H:%M:%S')
# print(yesterday_time.strftime("%A"))
# print(yesterday.strftime("%A"))
# time.sleep(1000)
date_stock_yesterday = '2022-10-17 00:00:00'
# print(stock_data_yesterday)
# time.sleep(1000)
f = open("stocks.txt", "w")
sql = "INSERT INTO stock_dailies (datetime,code,open,high,low,last,volume, prev, stock_logo) VALUES"
val = "("
for i in data:
    stock_data_yesterday = "(select close from stock_datas where stock_code = '" + i["Code"] +  "'" + "and date = '" + date_stock_yesterday + "')"
    stock_id = "(select uuid from stocks where code = '" + i["Code"] + "')"
    stock_data_yesterday_diff = "(" + str(i["Last"]) + " - (select close from stock_datas where stock_code = '" + i["Code"] +  "'" + "and date = '" + date_stock_yesterday + "')"
    stock_logo = "( select filename from stock_logos where stock_code = '" + i["Code"]  + "')"

    # print(type(i['Remarks']) )
    data = "'"+str(i["DateTime"]).replace("T", " ") + "'" + "," + "'" + i["Code"] + "'" + ","  +str(i["Open"])  + "," + str(i["High"]) + ","+ str(i["Low"])  + "," + str(i["Last"]) + ","+ str( i["Volume"]) + "," + stock_data_yesterday + ","+stock_logo
    command_val = "(" + data + ")" + ";"
    print(data)
    print(sql + command_val)
    # time.sleep(10000)
    mycursor.execute(sql + command_val)
    mydb.commit()
    # time.sleep(1000)

    
    # f.write(i["Date"])
    # f.write(",")
    # f.write(i["StockCode"])
    # f.write(",")
    # f.write(i["StockName"])
    # f.write(",")
    # f.write(i["Remarks"])
    # f.write(",")
    # f.write(str(i["Previous"]))
    # f.write(",")
    # f.write(str(i["OpenPrice"]))
    # f.write(",")
    # f.write(str(i["FirstTrade"]))
    # f.write(",")
    # f.write(str(i["High"]))
    # f.write(",")
    # f.write(str(i["Low"]))
    # f.write(",")
    # f.write(str(i["Close"]))
    # f.write(",")
    # f.write(str(i["Change"]))
    # f.write(",")
    # f.write(str(i["Volume"]))
    # f.write(",")
    # f.write(str(i["Value"]))
    # f.write(",")
    # f.write(str(i["Frequency"]))
    # f.write(",")
    # f.write(str(i["IndexIndividual"]))
    # f.write(",")
    # f.write(str(i["Offer"]))
    # f.write(",")
    # f.write(str(i["OfferVolume"]))
    # f.write(",")
    # f.write(str(i["Bid"]))
    # f.write(",")
    # f.write(str(i["BidVolume"]))
    # f.write(",")
    # f.write(str(i["ListedShares"]))
    # f.write(",")
    # f.write(str(i["TradebleShares"]))
    # f.write(",")
    # f.write(str(i["WeightForIndex"]))
    # f.write(",")
    # f.write(str(i["ForeignSell"]))
    # f.write(",")
    # f.write(str(i["ForeignBuy"]))
    # f.write(",")
    # f.write(i["DelistingDate"])
    # f.write(",")
    # f.write(str(i["NonRegularVolume"]))
    # f.write(",")
    # f.write(str(i["NonRegularValue"]))
    # f.write(",")
    # f.write(str(i["NonRegularFrequency"]))
    # f.write(",")
    # f.write(str(i["persen"]))
    # f.write(",")
    # f.write(str(i["percentage"]))
    # f.write("\n")

mydb.commit()
f.close()
