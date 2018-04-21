import requests
import numpy as np
import pandas as pd
import talib
import pyodbc
import logging
from datetime import datetime, timedelta
import threading
import math
import time
from bs4 import BeautifulSoup as BS
import os
import codes.codes as TWSE

class dbHandle( ):

    def __init__( self, server, database, username, password ):

        self.datelst = [ ]
        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )

class Technical_Indicator:

    """ 輸入線型周期, 查詢股價
        D 日線"""
    def __init__( self, num = '2497', item = 'D' , tree = '', **d ):

        path = { 'D' : num + '_日線.csv' }

        self.path = '{}/{}'.format( tree, path[ item ] )
        self.number = num
        self.df = pd.DataFrame( )
        self.type = item
        self.days = d[ item ]

    def GetDF( self ):

        url = "http://5850web.moneydj.com/z/BCD/czkc1.djbcd"
        querystring = { "a": self.number, "b": self.type, "c": self.days, "E": "1", "ver": "5" }

        headers = {
            'user-agent': "Chrome/64.0.3282.186",
            'content-type': "text/html;charset=big5",
            'accept': "*/*",
            'referer': "http://5850web.moneydj.com/z/zc/zcw/zcw1.DJHTM?A={}".format( self.number ),
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
            'cache-control': "no-cache", }

        try:
            response = requests.request( "GET", url, headers = headers, params = querystring )
        except Exception as e:
            print( response.url )

        data = response.text.split( ' ' )

        val = 0
        d = { 1:'日期', 2:'開盤', 3:'最高', 4:'最低', 5:'收盤', 6:'成交量' }

        for i in data:
            val += 1
            lst = i.split( ',' )
            if val == 1:
                self.df[ '日期' ] = lst
            elif val in d.keys( ):
                self.df[ d[ val ] ] = [ float( i ) for i in lst ]

        self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%Y/%m/%d" )
        return False

    def CombineDF( self ):

        try:
            df_read = pd.read_csv( self.path, sep = ',', encoding = 'utf8', false_values = 'NA', dtype={ '日期': str } )
            df_read = df_read[ [ '日期', '開盤', '最高', '最低', '收盤', '成交量'  ] ]

            df_read[ '日期' ] = pd.to_datetime( df_read[ '日期' ], format = "%y%m%d" )
            self.df = pd.concat( [ df_read, self.df ], ignore_index = True )

            self.df.drop_duplicates( [ '日期' ], keep = 'last', inplace = True )
            self.df.sort_values( by = '日期',  ascending=True, inplace = True )
            self.df.reset_index( drop = True, inplace = True )

        except Exception as e:
            pass
            # print( str( e ) )
            # logging.exception( e )
            # print( self.path, '首次存檔' )

    def SaveCSV( self ):

        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )
        self.df = self.df.round( decimals =  2 )
        self.df.to_csv( self.path, sep = ',', encoding = 'utf-8', date_format='%y%m%d' )

def _GetDay( obj ):
    # time.sleep( 1 )
    obj.GetDF( )
    obj.CombineDF( )
    obj.SaveCSV( )

def _DetStockIsNotExist( number ):

    time.sleep( 1 )
    url = "http://5850web.moneydj.com/z/zc/zcw/zcw1.DJHTM"

    querystring = { "A": number }

    headers = {
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/64.0.3282.186",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'accept-encoding': "gzip, deflate",
        'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        'cache-control': "no-cache", }

    response = requests.request( "GET", url, headers = headers, params = querystring )
    soup = BS( response.text, "html.parser" )
    print( '股號{:>7} 處理中'.format(number) )

    try:
        row = soup.find( 'center' ).text
    except AttributeError as e:
        return False

    if row == '所選個股代碼錯誤':
        print( row )
        return True
    else:
        return  False

def GetFile( *lst ):

    query = { 'D': 61 }
    lst = list( lst )
    path = 'C:/workspace/data/技術指標/'

    while lst:
        start_tmr = datetime.now( )
        stock = lst.pop( 0 )
        ti_D  = Technical_Indicator( stock, 'D', path, **query )

        try:
            _GetDay( ti_D )
        except Exception as e:
            print( '{}'.format( e ) )
            if '{}'.format( e ) != 'day is out of range for month':
                lst.append( stock )
                print( 'lst.append({})'.format( stock ) )

        consumption = datetime.now( ) - start_tmr
        print( '股號{0:>7} 耗時{1:>4} Second {2}'.format( stock, consumption.seconds, len( lst ) ) )

def CompareFileModifyTime( path, hour = 6 ):

    one_days_ago = datetime.now( ) - timedelta( hours = hour )
    try:
        filetime = datetime.fromtimestamp( os.path.getmtime( path ) )
        if filetime < one_days_ago:
            print( '{0:<20}修改 {1} 超過 {2} hour'.format( path, filetime.strftime( '%m-%d %H:%M' ), hour ) )
            return False
        else:
            return True
    except Exception as e:
        return False

def main( ):
    """範例http://5850web.moneydj.com/z/zc/zcw/zcw1_3312.djhtm"""
    try:
        db = dbHandle( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        db = dbHandle( 'localhost', 'StockDB', 'sa', '292929' )

    empty_lst = list( TWSE.codes.keys( ) )
    stock_lst = [ ]

    for stock in empty_lst:
        if CompareFileModifyTime( '{0}_日線.csv'.format( stock ), 12 ):
            continue
        stock_lst.append( stock )

    print( '股票更新{}支'.format( len( stock_lst ) ) )

    thread_count = 2
    thread_list = [ ]

    for i in range( thread_count ):
        start = math.floor( i * len( stock_lst ) / thread_count )
        end   = math.floor( ( i + 1 ) * len( stock_lst ) / thread_count )
        thread_list.append( threading.Thread( target = GetFile, args = stock_lst[ start:end ]  ) )

    for thread in thread_list:
        thread.start( )

    for thread in thread_list:
        thread.join( )

if __name__ == '__main__':

    start_time = time.time( )
    main( )
    print( 'The script took {:06.2f} minute !'.format( ( time.time( ) - start_time ) / 60  ) )