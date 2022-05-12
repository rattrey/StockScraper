

import os
os.chdir('C:\\Users\\Rohin\\Desktop\\Python\\Finance Project')

import plotly.graph_objs as go
import numpy as np
from general_functions import postgres_query_df
from general_functions import get_columns
import matplotlib.pyplot as plt
from datetime import date
from datetime import datetime
from mpl_toolkits import mplot3d
from sqlalchemy import create_engine
import sys
import pandas as pd
import plotly.express as px
from scraperRo import getTickerList
from general_functions import get_columns
from scraperRo import getTickerList

engine = create_engine('postgresql+psycopg2://attrey:tusr3f@127.0.0.1/postgres')
#Determine time-period performances for each stock and append them back into postgres



all_stock_data = postgres_query_df('''select * from public."dailyData" where ticker = 'SHOP';''')
all_stock_data.columns = get_columns('dailyData')


#all_stock_data.columns = ['date', 'adjusted_close', 'ticker']
all_stock_data = all_stock_data.sort_values(by = ['ticker','date'])

counter = len(set(all_stock_data['ticker']))
for ticker in set(all_stock_data['ticker']):
    try:
        t2 = all_stock_data[all_stock_data['ticker'] == f'{ticker}'].reset_index(drop=True)

        x = lambda a, b: (int(pd.to_numeric(t2[b:]['adjusted_close']).tail(a)) / int(pd.to_numeric(t2[b:]['adjusted_close']).head(a))) - 1
        stock_performance = pd.DataFrame({'ticker': ticker, 'Day_Performance_5': [x(1,-5)], 'Day_Performance_30': [x(1, -30)], 'Day_Performance_90': [x(1,-90)], 'Day_Performance_180': [x(1, -180)], 'Year_Performance_1':[x(1,-365)], 'Year_Performance_5':[x(1,-5*365)]})
        stock_performance.to_sql('stock_performance_test', engine, if_exists='append')

        counter -= 1
        print(ticker, counter)
    except Exception as error:
        print(f'{ticker} produced an error: {error}')
print(f'Complete')


##############Plotting time period performances for every stock#########
#Section 2 - pull sector data and clean to prepare for plotting and analysis
sector_data = postgres_query_df("select * from public.sector_data;")
sector_data.columns = get_columns('sector_data')

sector_data1 = sector_data.transpose()[1:]
sector_data1.columns = sector_data.transpose().iloc[0]
sector_data1['Metric'] = sector_data1.index
sector_data1
########## Bar plot: growth over time by industry### Use this to determine which industry to follow
sector_data2 = sector_data1.drop(sector_data1.index[[-1,-2]])
sector_data2.head()


x = sector_data2.loc[:,'Metric']

fig = go.Figure(data=[
    go.Line(name='Communication Services', x=x, y=list(sector_data2.loc[:,'Communication Services'])),
    go.Line(name = 'Consumer Discretionary', x = x, y = list(sector_data2.loc[:,'Consumer Discretionary'])),
    go.Line(name = 'Consumer Staples', x = x, y = list(sector_data2.loc[:,'Consumer Staples'])),
    go.Line(name = 'Energy', x = x, y = list(sector_data2.loc[:,'Energy'])),
    go.Line(name = 'Financials', x = x, y = list(sector_data2.loc[:,'Financials'])),
    go.Line(name = 'Health Care', x = x, y = list(sector_data2.loc[:,'Health Care'])),
    go.Line(name = 'Industrials', x = x, y = list(sector_data2.loc[:,'Industrials'])),
    go.Line(name = 'Information Technology', x = x, y = list(sector_data2.loc[:,'Information Technology'])),
    go.Line(name = 'Materials', x = x, y = list(sector_data2.loc[:,'Materials'])),
    go.Line(name = 'Real Estate', x = x, y = list(sector_data2.loc[:,'Real Estate'])),
    go.Line(name = 'Utilities', x = x, y = list(sector_data2.loc[:,'Utilities']))
    
])
fig.update_layout(barmode='group')
fig.show()


##### 3D Scatterplot#####
stock_performance = postgres_query_df('select * from stock_performance_6 where industry IS NOT NULL;')
stock_performance.columns = get_columns('stock_performance_6')
fig = px.scatter_3d(stock_performance, x = 'Day_Performance_30', y = 'Day_Performance_90', z = 'Day_Performance_180', color = 'industry', hover_name = 'ticker', hover_data=['ticker'])
fig.show()


stock_performance.head()



x = getTickerList('tickerList')
x.sort()
x