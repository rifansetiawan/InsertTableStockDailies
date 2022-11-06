import csv
import time
import datetime
from datetime import date
from datetime import datetime, timedelta

now = datetime.now()
month_now = now.strftime("%b%Y")
date_now = now.strftime("%m/%-d/%Y")
print(date_now)
print(month_now)
time.sleep(1000)
header_temp = ['Ticker','Date/Time','Open','High','Low','Close','Volume','OpenInt','OpenInt']
data = []
with open('daily_nov.csv') as f:
    cf = csv.reader(f)
    next(cf)
    for row in cf:
        print(row)
        print(row[1][0:-9])
        if row[1][0:-9] != date_now:
            data.append(row)
        # time.sleep(1000)
with open('historical_temp.csv', 'w', encoding='UTF8', newline='') as f_tmp:
    writer = csv.writer(f_tmp)
    writer.writerow(header_temp)
    writer.writerows(data)