import csv
f=open("logocsv.csv")
for row in csv.reader(f):
    print(row)