
import os
os.chdir('C:\\Users\\Rohin\\Desktop\\Python\\Finance Project')

import plotly.graph_objs as go
import numpy as np
from general_functions import postgres_query_df
from general_functions import get_columns
from scraperRo import getTickerList
from general_functions import get_columns

import numpy as np
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
import time
import psycopg2
import datetime
from datetime import datetime
import alpha_vantage
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.sectorperformance import SectorPerformances
import pandas as pd
from sqlalchemy import create_engine
import sys
from general_functions import postgres_query_df
from general_functions import postgres_query_list
from general_functions import find_nth
from datetime import date
import re
from sqlalchemy import create_engine
import sys
from scraperRo import getTickerList
from scraperRo import scrapeDailyData
from stockFunctions import growth
from stockFunctions import posByPeriod
from scstockFunctionsrap import negByPeriod
from stockFunctions import avgGrowthByPeriod

engine = create_engine('postgresql+psycopg2://attrey:tusr3f@127.0.0.1/postgres')


MstrLst = getTickerList('tickerList')
counter = len(MstrLst)


for ticker in MstrLst:

    try:
        table = postgres_query_df(f'''SELECT * FROM public."DailyData2" where ticker = '{ticker}';''')

        df = table.iloc[:]

        df.columns = ['Date', 'open', 'high', 'low', 'close', 'adjusted_close','volume', 'dividend_amount', 'split_coefficient', 'ticker']
        df = df.sort_values(by = ['Date'])

        #######################################################################################################################
        l = df['close'].tolist()

        x = pd.merge(posByPeriod(l, 10, f'{ticker}'), negByPeriod(l, 10, f'{ticker}'), on = 'ticker')
        df = pd.merge(x, avgGrowthByPeriod(l, 10, f'{ticker}'), on = 'ticker')
        df = df.set_index('ticker').reset_index()
        df['historyYr'] = len(l)/365
        df['GrowthSinceInception'] = growth(l, len(l) - 1)
        df['CurrentPrice'] = l[-1]
        
        df.to_sql(f'growth', engine, if_exists='append')
        counter -= 1
        print(counter, ticker)
    except Exception:
        pass


