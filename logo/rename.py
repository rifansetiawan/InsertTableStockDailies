import os
import csv
# Absolute path of a file
# old_name = r"E:\demos\files\reports\details.txt"
# new_name = r"E:\demos\files\reports\new_details.txt"

f=open("logocsv.csv")
for row in csv.reader(f):
    old_name = os.getcwd() + "/" + row[0]
    new_name = os.getcwd() + "/" + row[1]
    os.rename(old_name, new_name)

    print(old_name)
    print(new_name)
    print(row)

# Renaming the file
# os.rename(old_name, new_name)