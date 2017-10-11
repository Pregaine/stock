# -*- coding: utf-8 -*-

from datetime import datetime
import os
import os.path
import re
import sys
import pandas as pd
import numpy as np
import csv
import pyodbc

# Todo
# 建立 Table
# date, 股號, top 15 買均價 top 15賣均價 top 15 買進金額 top 15賣出金額
# 1日 5日 10日 20日 30日 45日 60日 120日

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

    # def __del__( self ):
    #     self.con_db.close( )

    def GetDates( self, num, days ):

        cmd = '''SELECT date.date
                FROM DailyTrades dt
                inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
                inner JOIN Stocks stk 	ON dt.stock_id      = stk.id
                inner JOIN ( SELECT top( ''' + days + ''' ) * FROM Dates ORDER BY Dates.date desc) date ON dt.date_id = date.id
                WHERE stk.symbol = ''' + num + '''
                GROUP BY date.date '''

        ft = self.cur_db.execute( cmd ).fetchall( )

        # print( days, len( ft ) )

        date = [ elt[ 0 ] for elt in ft ]

        self.datelst = [ val.strftime( "%Y/%m/%d" ) for val in sorted( date ) ]

        self.datelst =  self.datelst[ ::-1 ]

    def GetVolume( self, stock_symbol, days ):

        cmd = '''--total deal vol
        SELECT sum( dt.sell_volume) / 1000 as total_buy_volume	
        FROM DailyTrades dt
        inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
 		inner JOIN (SELECT top( ''' + days + ''' ) * FROM Dates ORDER BY Dates.date desc) date ON dt.date_id = date.id 
        inner JOIN Stocks stk ON dt.stock_id = stk.id		
  		where stk.symbol = ''' + stock_symbol

        ft = self.cur_db.execute( cmd ).fetchall( )

        return ft

    def GetVolumeBetweenDay( self, symbol, start, end ):

        start = '\'' + start + '\''
        end   = '\'' + end + '\''

        cmd = '''--total deal vol
        SELECT sum( CONVERT( bigint, dt.sell_volume ) ) / 1000 as total_buy_volume	
        FROM DailyTrades dt
        inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
        inner JOIN ( SELECT * FROM Dates where date between ''' + start + ''' and ''' + end + ''' ) date ON dt.date_id = date.id
        inner JOIN Stocks stk ON dt.stock_id = stk.id		
        where stk.symbol = ''' + symbol

        ft = self.cur_db.execute( cmd ).fetchall( )

        return ft[ 0 ][ 0 ]

    def Get_BetweenDayList( self, interval ):

        tmplst = self.datelst[ : ]
        outlst = [ ]

        while len( tmplst ) > interval:

            outlst.append( ( tmplst[ 0 ], tmplst[ interval - 1 ] ) )

            tmplst.pop( 0 )

        return outlst

    def GetStockList( self ):
        cmd = '''SELECT [symbol] FROM [StockDB].[dbo].[Stocks]'''

        ft = self.cur_db.execute( cmd ).fetchall( )

        return [ val[ 0 ] for val in ft ]

    def GetTopSell( self, stock_symbol, days ):

        cmd = '''
        --sell top 15 
        select sum(buy_sell_price) buy_sell_price ,sum(buy_sell_vol) buy_sell_vol from
        (
            -- top sell view
            -- get top 15 broker
            SELECT top( 15 )  
            dt.stock_id,
            bk.symbol as brokerage_sym,
            bk.name as brokerage_name,		
            sum( dt.sell_volume * dt.price ) - sum( dt.buy_volume * dt.price ) as buy_sell_price,
            sum( dt.sell_volume - dt.buy_volume ) / 1000 as buy_sell_vol                 
            FROM DailyTrades dt
            inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
            inner JOIN (SELECT top( ''' + days + ''' ) * FROM Dates ORDER BY Dates.date desc) date 	ON dt.date_id = date.id -- get last 15 days
            inner JOIN Stocks stk 	ON dt.stock_id = stk.id		
            where stk.symbol = ''' + stock_symbol + '''
            GROUP BY dt.stock_id,bk.symbol,bk.name
            ORDER BY sum( dt.sell_volume * dt.price ) - sum( dt.buy_volume * dt.price ) DESC
        ) a'''

        ft = self.cur_db.execute( cmd ).fetchall( )

        return ft

    def GetTopBuy( self, stock_symbol, days ):

        # buy top 15
        cmd = '''
        -- buy top 15 
        select sum(buy_sell_price) buy_sell_price ,sum(buy_sell_vol) buy_sell_vol from
        (
        -- top buy view
        -- get top 15 broker 
        SELECT top( 15 ) 
                dt.stock_id,
                bk.symbol as brokerage_sym,
                bk.name as brokerage_name,		
                sum( dt.buy_volume * dt.price ) - sum( dt.sell_volume * dt.price ) as buy_sell_price,
                sum( dt.buy_volume - dt.sell_volume ) / 1000 as buy_sell_vol                 
                FROM DailyTrades dt
                inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
                inner JOIN ( SELECT top( ''' + days + ''' ) * FROM Dates ORDER BY Dates.date desc) date ON dt.date_id = date.id --get last 15 days
                inner JOIN Stocks stk ON dt.stock_id = stk.id		
                where stk.symbol = ''' + stock_symbol + '''
                GROUP BY dt.stock_id,bk.symbol,bk.name
                ORDER BY sum( dt.buy_volume * dt.price ) - sum( dt.sell_volume * dt.price ) DESC
        ) a '''

        ft = self.cur_db.execute( cmd ).fetchall( )

        return ft

    def GetTopBuyBetweenDay(self, symbol, start_day, end_day ):

        start_day = '\'' + start_day + '\''
        end_day   = '\'' + end_day + '\''

        # buy top 15
        cmd = '''
        -- buy top 15 
        select sum( buy_sell_price ) buy_sell_price, sum( buy_sell_vol ) buy_sell_vol from
        (
        -- top buy view
        -- get top 15 broker
            SELECT top( 15 )  
                dt.stock_id,
                bk.symbol as brokerage_sym,
                bk.name as brokerage_name,		
                sum( dt.buy_volume * dt.price ) - sum( dt.sell_volume * dt.price ) as buy_sell_price,
                sum( dt.buy_volume - dt.sell_volume ) / 1000 as buy_sell_vol                 
                FROM DailyTrades dt
                inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
                inner JOIN ( SELECT * FROM Dates where date between ''' + start_day + ''' and ''' + end_day + ''' ) date ON dt.date_id = date.id
                inner JOIN Stocks stk ON dt.stock_id = stk.id		
                where stk.symbol = ''' + symbol + '''
                GROUP BY dt.stock_id,bk.symbol,bk.name
                ORDER BY sum( dt.buy_volume * dt.price ) - sum( dt.sell_volume * dt.price ) DESC
        ) a '''

        ft = self.cur_db.execute( cmd ).fetchall( )

        # price = ft[ 0 ][ 0 ]
        vol   = ft[ 0 ][ 1 ]

        return vol

    def GetTopSellBetweenDay(self, symbol, start_day, end_day ):

        start_day = '\'' + start_day + '\''
        end_day   = '\'' + end_day + '\''

        cmd = '''
                --sell top 15 
                select sum(buy_sell_price) buy_sell_price, sum(buy_sell_vol) buy_sell_vol from
                (
                    -- top sell view
                    -- get top 15 broker
                    SELECT top( 15 )  
                    dt.stock_id,
                    bk.symbol as brokerage_sym,
                    bk.name as brokerage_name,		
                    sum( dt.sell_volume * dt.price ) - sum( dt.buy_volume * dt.price ) as buy_sell_price,
                    sum( dt.sell_volume - dt.buy_volume ) / 1000 as buy_sell_vol                 
                    FROM DailyTrades dt
                    inner JOIN Brokerages bk ON dt.brokerage_id = bk.id
                    inner JOIN ( SELECT * FROM Dates where date between ''' + start_day + ''' and ''' + end_day + ''' ) date ON dt.date_id = date.id
                    inner JOIN Stocks stk 	ON dt.stock_id = stk.id		
                    where stk.symbol = ''' + symbol + '''
                    GROUP BY dt.stock_id,bk.symbol,bk.name
                    ORDER BY sum( dt.sell_volume * dt.price ) - sum( dt.buy_volume * dt.price ) DESC
                ) a'''

        ft = self.cur_db.execute( cmd ).fetchall( )

        # price = ft[ 0 ][ 0 ]
        vol   = ft[ 0 ][ 1 ]

        return vol

    def GetConcentrateBetweenDay( self, symbol, end, start ):

        buy_vol = self.GetTopBuyBetweenDay( symbol, start, end )

        sell_vol = self.GetTopSellBetweenDay( symbol, start, end )

        sum_vol = self.GetVolumeBetweenDay( symbol, start, end )

        print( '集中度', symbol, end, start, buy_vol, sell_vol, sum_vol )

        if buy_vol is not 0 and sell_vol is not 0 and sum_vol is not 0:
            return ( ( buy_vol - sell_vol ) / sum_vol ) * 100
        else:
            return None

    def GetConcentrate( self, num, interval ):

        buy = self.GetTopBuy( num, interval )

        print( '買/量', buy )

        buy_lst = buy[ 0 ]

        sell = self.GetTopSell( num, interval )

        print( '賣/量', sell )
        sell_lst = sell[ 0 ]

        sum_val = self.GetVolume(  num, interval )

        sum_lst = sum_val[ 0 ]

        return ( buy_lst[ 1 ] - sell_lst[ 1 ] ) / sum_lst[ 0 ]

def unit( ):

    start_tmr = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = '292929'
    days = 1
    tmp_file = '籌碼集中暫存_1.csv'

    db = dbHandle( server, database, username, password )

    stock_lst = db.GetStockList( )[  0: ]

    cols = [ '日期', '股號', '01天集中度', '03天集中度', '05天集中度', '10天集中度', '20天集中度', '60天集中度' ]

    df = pd.read_csv( tmp_file, sep = ',', encoding = 'utf8', false_values = 'NA',
                      names = cols, dtype={ '股號': str } )

    tmp_lst = df[ '股號' ].tolist( )

    if len( tmp_lst ) is not 0:
        src_lst = list( set( stock_lst ) - set( tmp_lst ) )
        src_lst.insert( 0, tmp_lst[ -1 ] )
    else:
        src_lst = stock_lst

    print( '上次捉取', len( df[ '股號' ].tolist( ) ), len( src_lst ), len( stock_lst ) )

    for stock in sorted( src_lst ):

        db.GetDates( stock, '121' )

        day01_lst = db.Get_BetweenDayList( 1 )
        day03_lst = db.Get_BetweenDayList( 3 )
        day05_lst = db.Get_BetweenDayList( 5 )
        day10_lst = db.Get_BetweenDayList( 10 )
        day20_lst = db.Get_BetweenDayList( 20 )
        day60_lst = db.Get_BetweenDayList( 60 )

        for val in range( days ):

            try:
                df_01 = db.GetConcentrateBetweenDay( stock, day01_lst[ val ][ 0 ], day01_lst[ val ][ 1 ] )
            except:
                df_01 = None
            try:
                df_03 = db.GetConcentrateBetweenDay( stock, day03_lst[ val ][ 0 ], day03_lst[ val ][ 1 ] )
            except:
                df_03 = None
            try:
                df_05 = db.GetConcentrateBetweenDay( stock, day05_lst[ val ][ 0 ], day05_lst[ val ][ 1 ] )
            except:
                df_05 = None
            try:
                df_10 = db.GetConcentrateBetweenDay( stock, day10_lst[ val ][ 0 ], day10_lst[ val ][ 1 ] )
            except:
                df_10 = None
            try:
                df_20 = db.GetConcentrateBetweenDay( stock, day20_lst[ val ][ 0 ], day20_lst[ val ][ 1 ] )
            except:
                df_20 = None
            try:
                df_60 = db.GetConcentrateBetweenDay( stock, day60_lst[ val ][ 0 ], day60_lst[ val ][ 1 ]  )
            except:
                df_60 = None

            try:
                row = (day01_lst[val][0], stock, df_01, df_03, df_05, df_10, df_20, df_60)
                print('寫入', row)
            except:
                print(stock, '無日期')
                continue

            with open( '籌碼集中暫存_1.csv', 'a', newline = '\n', encoding = 'utf8' ) as csv_file:
                file = csv.writer( csv_file, delimiter = ',' )
                file.writerow( row )

    result = pd.read_csv( tmp_file, sep = ',', encoding = 'utf8', false_values = 'NA',
                          names = cols, dtype={ '股號': str } )

    result.drop_duplicates( [ '日期', '股號' ], keep = 'last', inplace = True )

    result.sort_values( by = [ '日期', '股號' ], ascending = [ False, True ], inplace =  True )

    result = result.reset_index( drop=True )

    df_writer = pd.ExcelWriter( '籌碼集中單元測試_20171006.xlsx' )
    result.to_excel( df_writer, sheet_name = '籌碼分析' )

    print( datetime.now( ) - start_tmr )

def main( ):

    result = pd.DataFrame( )
    start_tmr = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    db = dbHandle( server, database, username, password )

    stock_lst = db.GetStockList( )

    for stock in stock_lst:

        df_01  = db.GetConcentrate( stock, '1' )
        df_05  = db.GetConcentrate( stock, '5' )
        df_15  = db.GetConcentrate( stock, '15' )
        df_30  = db.GetConcentrate( stock, '30' )
        df_60  = db.GetConcentrate( stock, '60' )
        df_120 = db.GetConcentrate( stock, '120' )

        df_tmp = pd.DataFrame( {

                '01天集中度': df_01,
                '05天集中度': df_05,
                '15天集中度': df_15,
                '30天集中度': df_30,
                '60天集中度': df_60,
                '120天集中度': df_120,
                '股號': stock

                }, index = [ 0 ] )

        result = pd.concat( [ result, df_tmp ] )

    cols = [ '股號', '01天集中度', '05天集中度', '15天集中度', '30天集中度', '60天集中度', '120天集中度' ]

    result = result.reindex( columns = cols )

    df_writer = pd.ExcelWriter( '籌碼集中.xlsx' )
    result.to_excel( df_writer, sheet_name = '籌碼分析' )

    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':
    # main( )
    unit( )