import pandas as pd
import urllib
import datetime
import os
import re
import numpy
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy
import datetime



downloadpath = '/Users/Red/Google Drive/Python Projects/Stocks2/Downloaded/%s.csv'

try:
  cnx = mysql.connector.connect(user='Scrambles', password='6c0f1c444f', host='192.168.1.121', database='stocks')
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
cursor = cnx.cursor()
 
def alreadyInSQL():
    tickerlist = []
    cursor.execute("SHOW TABLES")  
    rawlist = cursor.fetchall()
    for i in range(len(rawlist)):
        tickerlist.append(rawlist[i][0].upper())
    #cursor.close()
    return tickerlist
    

def timeFormat(timelist):
    dates = []
    for time in timelist:
        try:
            int(time[0])
            dates.append((datetime.datetime.fromtimestamp(newTimeStamp + (int(time) * 60)) + datetime.timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'))
        except ValueError:
            newTimeStamp = int(time[1:]) - (240 * 60)
            dates.append((datetime.datetime.fromtimestamp(newTimeStamp) + datetime.timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'))
    return dates


def changeCalc(df):
    thatsnotchange = [0]
    for i in range(1,len(df['close'])):
        thatsnotchange.append(float(df.close[i])/float(df.close[i-1])-1)
    return thatsnotchange


def downloader(ticker, action):
    if action == 'update':
        df2 = pd.read_sql_query('SELECT * FROM `%s` order by `index` desc limit 1' % ticker, cnx)
        a = datetime.datetime.now() - df2.date
        url='https://www.google.com/finance/getprices?i=60&p=%(1)sd&f=d,o,h,l,c,v&df=cpct&q=%(2)s' % {"1" : a[0].days+1, "2" : ticker.upper()}
        urllib.urlretrieve(url, downloadpath % ticker)
        return df2.date[0]
    elif action == 'insert':
        url='https://www.google.com/finance/getprices?i=60&p=20d&f=d,o,h,l,c,v&df=cpct&q=%s' % ticker.upper()
        urllib.urlretrieve(url, downloadpath % ticker)
        return
    

def updater(ticker, action):
    cutdate = downloader(ticker, action)
    try:
        df = pd.read_csv(downloadpath % ticker, skiprows=7, header=None)
    except:
        print("Could not download %s from Google" % ticker)
        return
    df.columns = ['date','close','high','low','open','volume']
    df.date = timeFormat(list(df.date))
    df = df.set_index(pd.DatetimeIndex(df['date']))
    df['change'] = changeCalc(df)
    if action == 'update':
        try:
            df = df.loc[cutdate:]
            df = df.drop([cutdate])
        except ValueError:
            pass
    if df.empty:
        print('No new data in %s' % ticker.upper())
        return
    else:
        df.to_sql(name = "\"%s\"" % ticker.lower(), con=cnx, flavor='mysql', if_exists='append')
        print('%(1)s new lines written to %(2)s' % {"1" : len(df.index), "2" : ticker.upper()})
        return
        

def insertDB():
    df = pd.read_csv('/Users/Red/Google Drive/Python Projects/Stocks2/NASDAQ.csv')
    df['Symbol'] = df['Symbol'].astype(str)
    df['Symbol'] = df['Symbol'].str.strip()
    tickerlist = df['Symbol'].tolist()
    df = pd.read_csv('/Users/Red/Google Drive/Python Projects/Stocks2/NYSE.csv')
    df['Symbol'] = df['Symbol'].astype(str)
    df['Symbol'] = df['Symbol'].str.strip()
    tickerlist2 = df['Symbol'].tolist()
    tickerlist = tickerlist +  tickerlist2
    tmp = alreadyInSQL()
    tickerlist = [x for x in tickerlist if x not in tmp]
    print('Initiating insert of %s tickers into SQL Database' % len(tickerlist))
    for ticker in tickerlist[530:]:
        updater(ticker, 'insert')
        print('%(1)s out of %(2)s insert operations complete.' % {"1" : tickerlist.index(ticker), "2" : len(tickerlist)})
        print '***'
    return
    
def updateDB():
    tickerlist = alreadyInSQL()
    print('Initiating update of %s tickers in SQL Database' % len(tickerlist))
    for ticker in tickerlist:
        updater(ticker, 'update')
        print('%(1)s out of %(2)s update operations complete.' % {"1" : tickerlist.index(ticker), "2" : len(tickerlist)})
        print '***'
    return
    
insertDB()
updateDB()

