import pandas as pd

df = pd.read_csv('daily.csv')

df_agg=df.groupby("Ticker").last()

# print(df_agg)
for idx,x in df_agg.iterrows():
    print(x['Date/Time'])