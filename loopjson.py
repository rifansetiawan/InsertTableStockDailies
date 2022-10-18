import json
import mysql.connector
import time

mydb = mysql.connector.connect(
  host="localhost",
  user="rifan",
  password="1234",
  database="pasardanamobile"
)

print(mydb)

mycursor = mydb.cursor()
f = open('stock_intra_daily_17102022.json')

data = json.load(f)
f = open("stocks.txt", "w")
sql = "INSERT INTO stock_datas (date,stock_code,stock_name,remarks,previous,open,first_trade,high,low,close,changes,volume,value,frequency,index_individual,offer,offer_volume,bid,bid_volume,listed_shares,tradable_shares,weight_for_index,foreign_cell,foreign_buy,non_reguler_volume,non_reguler_value,non_reguler_frequency) VALUES"
val = "("
for i in data:
    print(type(i['Remarks']) )
    data =  "'" + str(i["Date"]).replace("T", " ") + "'" + "," + "'" + i["StockCode"] + "'" + ","  + "'"+ i["StockName"] + "'" + ","+ "'" + str(i["Remarks"]) + "'"+ ","+ str(i["Previous"])  + "," + str(i["OpenPrice"]) + ","+ str(i["FirstTrade"])  + "," + str(i["High"]) + ","+ str( i["Low"])  + "," + str(i["Close"]) + ","+ str(i["Change"])  + "," + str(i["Volume"]) + ","+ str(i["Value"])  + "," + str(i["Frequency"]) + ","+ str(i["IndexIndividual"])  + "," + str(i["Offer"]) + ","+ str(i["OfferVolume"])  + "," + str(i["Bid"]) + ","+ str(i["BidVolume"])  + "," + str(i["ListedShares"]) + ","+ str(i["TradebleShares"])  + "," + str(i["WeightForIndex"]) + ","+ str(i["ForeignSell"])  + "," + str(i["ForeignBuy"]) + "," + str(i["NonRegularVolume"]) + ","+ str(i["NonRegularValue"])  + "," + str(i["NonRegularFrequency"])

    command_val = "(" + data + ")" + ";"
    print(data)
    print(sql + command_val)
    mycursor.execute(sql + command_val)
    mydb.commit()
    # time.sleep(1000)

    
    f.write(i["Date"])
    f.write(",")
    f.write(i["StockCode"])
    f.write(",")
    f.write(i["StockName"])
    f.write(",")
    f.write(i["Remarks"])
    f.write(",")
    f.write(str(i["Previous"]))
    f.write(",")
    f.write(str(i["OpenPrice"]))
    f.write(",")
    f.write(str(i["FirstTrade"]))
    f.write(",")
    f.write(str(i["High"]))
    f.write(",")
    f.write(str(i["Low"]))
    f.write(",")
    f.write(str(i["Close"]))
    f.write(",")
    f.write(str(i["Change"]))
    f.write(",")
    f.write(str(i["Volume"]))
    f.write(",")
    f.write(str(i["Value"]))
    f.write(",")
    f.write(str(i["Frequency"]))
    f.write(",")
    f.write(str(i["IndexIndividual"]))
    f.write(",")
    f.write(str(i["Offer"]))
    f.write(",")
    f.write(str(i["OfferVolume"]))
    f.write(",")
    f.write(str(i["Bid"]))
    f.write(",")
    f.write(str(i["BidVolume"]))
    f.write(",")
    f.write(str(i["ListedShares"]))
    f.write(",")
    f.write(str(i["TradebleShares"]))
    f.write(",")
    f.write(str(i["WeightForIndex"]))
    f.write(",")
    f.write(str(i["ForeignSell"]))
    f.write(",")
    f.write(str(i["ForeignBuy"]))
    f.write(",")
    f.write(i["DelistingDate"])
    f.write(",")
    f.write(str(i["NonRegularVolume"]))
    f.write(",")
    f.write(str(i["NonRegularValue"]))
    f.write(",")
    f.write(str(i["NonRegularFrequency"]))
    f.write(",")
    f.write(str(i["persen"]))
    f.write(",")
    f.write(str(i["percentage"]))
    f.write("\n")


f.close()
