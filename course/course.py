import pandas as pd
import matplotlib.pyplot as plt

# csv file two  column  date, point
close = pd.read_csv( 'taiex.csv', index_col = 'Date', parse_dates = True)['Point']

# print( close.head() )

sma = close.rolling(30).mean()
up_sma = sma * 1.01
dn_sma = sma * 0.99
# close[ '2015' ].plot(linewidth=1)
# sma[ '2015' ].plot()
# up_sma[ '2015' ].plot()
# dn_sma[ '2015' ].plot()
#
buy = close > up_sma
# buy.astype(int)['2015'].plot(secondary_y=True)
#
sell = close < dn_sma
# sell.astype(int)['2015'].plot(secondary_y=True)
#
hold = pd.Series( [0]*len(buy), index = buy.index)
hold[buy]= 1
hold[sell]=-1
hold=hold[hold!=0].reindex(hold.index).ffill()
#
# buy['2015'].astype(int).plot()
# sell['2015'].astype(int).plot()
# hold['2015'].astype(int).plot()
#
# close['2015':'2016'].plot()
# hold['2015':'2016'].plot(secondary_y=True)
#
(close.shift(-1)/close)[hold==1].cumprod().plot()

plt.show()