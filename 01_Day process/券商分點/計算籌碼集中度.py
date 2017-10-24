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

class dbHandle:

    def __init__( self, server, database, username, password ):

        self.date_lst = [ ]
        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )

    def ResetTable(self, table ):

       cmd = 'DROP TABLE IF EXISTS ' + table

       with self.cur_db.execute( cmd ):
            print( 'Successfully Deleter ' + table )

    def CreateTable(self):

        cmd = '''CREATE TABLE dbo.Concentrate

	(
	stock int NOT NULL,
	date date NOT NULL,
	one decimal(6, 2) NULL,
	three decimal(6, 2) NULL,
	five decimal(6, 2) NULL,
	ten decimal(6, 2) NULL,
	twenty decimal(6, 2) NULL,
	sixty decimal(6, 2) NULL
	)  ON [PRIMARY]

    CREATE NONCLUSTERED INDEX IX_Table_stock ON dbo.Concentrate
	(
	stock
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE NONCLUSTERED INDEX IX_Table_date ON dbo.Concentrate
	(
	date
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Concentrate SET (LOCK_ESCALATION = TABLE)

    COMMIT'''

        self.cur_db.execute( cmd )

    def Write( self, row ):

        stock = row[ 0 ]
        date = '\'' + row[ 1 ] + '\''

        cmd = 'SELECT * FROM Concentrate WHERE date = {} and stock = {} '.format( date, stock )

        ft = self.cur_db.execute( cmd ).fetchone( )

        if ft is None:

            var_string = ', '.join( '?' * ( len( row ) ) )

            cmd = 'INSERT INTO Concentrate VALUES ( {} )'.format( var_string )

            with self.cur_db.execute( cmd, row ):
                print( '寫入資料庫', row  )

            self.con_db.commit( )

    def GetDates( self, num, days ):

        cmd = 'SELECT TOP ( {0} ) date FROM DailyTrade WHERE stock = {1} ' \
              'GROUP BY date ORDER BY date desc'.format( days, num )

        ft = self.cur_db.execute( cmd ).fetchall( )

        date = [ elt[ 0 ] for elt in ft ]

        self.date_lst = [ val.strftime( "%Y/%m/%d" ) for val in sorted( date, reverse = True ) ]

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

        cmd = ''' 
        SELECT sum( CONVERT( bigint, volume ) ) as volume
        FROM TechAnaly_D
        where stock = {} and date between {} and {}
        Group By stock'''.format( symbol, start, end )

        ft = self.cur_db.execute( cmd ).fetchall( )

        return ft[ 0 ][ 0 ]

    def Get_BetweenDayList( self, interval ):

        # copy list from self
        cpy_lst = self.date_lst[ : ]

        ret_list = [ ]

        while len( cpy_lst ) > interval:

            ret_list.append( ( cpy_lst[ 0 ], cpy_lst[ interval - 1 ] ) )

            cpy_lst.pop( 0 )

        return ret_list

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
        SELECT top( 15 )
        sum( buy_volume * price ) - sum( sell_volume * price ) as buy_sell_price,
        sum( buy_volume - sell_volume ) / 1000 as buy_sell_vol
        FROM DailyTrade
        WHERE stock = {0} AND ( date between {1} and {2} )
        GROUP BY stock, brokerage
        ORDER BY buy_sell_price DESC'''.format( symbol, start_day, end_day )

        ft = self.cur_db.execute( cmd ).fetchall( )

        # price = ft[ 0 ][ 0 ]
        vol = ft[ 0 ][ 1 ]

        return vol

    def GetTopSellBetweenDay(self, symbol, start_day, end_day ):

        start_day = '\'' + start_day + '\''
        end_day   = '\'' + end_day + '\''

        cmd = '''
        SELECT top( 15 )  	
        sum( sell_volume * price ) - sum( buy_volume * price ) as buy_sell_price,
        sum( sell_volume - buy_volume ) / 1000 as buy_sell_vol                 
        FROM DailyTrade	
        where stock = {0} and date between {1} and {2}
        GROUP BY stock, brokerage
        ORDER BY buy_sell_price DESC
        '''.format( symbol, start_day, end_day )

        ft = self.cur_db.execute( cmd ).fetchall( )

        # price = ft[ 0 ][ 0 ]
        vol   = ft[ 0 ][ 1 ]

        return vol

    def GetConcentrateBetweenDay( self, symbol, end, start ):

        buy_vol = self.GetTopBuyBetweenDay( symbol, start, end )

        sell_vol = self.GetTopSellBetweenDay( symbol, start, end )

        sum_vol = self.GetVolumeBetweenDay( symbol, start, end )

        concentrate = None

        if sum_vol is not 0:
            concentrate =  ( ( buy_vol - sell_vol ) / sum_vol ) * 100

        row = '{} ~ {} 買入總成交量{:10d} 賣出總成交量{:10d} 總成交量{:10d} 集中度 {:3.2f}'. \
            format( end, start, buy_vol, sell_vol, sum_vol, concentrate )

        print( '集中度', symbol, row )

        return concentrate

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

def unit( tar_file ):

    start_tmr = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    # 回推天數，計算集中度
    days = 1

    tmp = '籌碼集中暫存.csv'

    db = dbHandle( server, database, username, password )

    # db.ResetTable( 'Concentrate' )
    # db.CreateTable()

    stock_lst = db.GetStockList( )[  0: ]

    if os.path.isfile( tmp ) is False:
        with open( tmp, 'wb' ) as f:
            csv.writer( f )

    cols = [ '日期', '股號', '01天集中度', '03天集中度', '05天集中度', '10天集中度', '20天集中度', '60天集中度' ]

    df = pd.read_csv( tmp, sep = ',', encoding = 'utf8', false_values = 'NA',
                      names = cols, dtype={ '股號': str } )

    tmp_lst = df[ '股號' ].tolist( )

    if len( tmp_lst ) is not 0:
        src_lst = list( set( stock_lst ) - set( tmp_lst ) )
        src_lst.insert( 0, tmp_lst[ -1 ] )
    else:
        src_lst = stock_lst

    print( '籌碼集中進度剩餘 {} 筆'.format(len( src_lst ))  )

    # for stock in sorted( src_lst ):
    for stock in [ '2330' ]:

        db.GetDates( stock, '61' )

        day01_lst = db.Get_BetweenDayList( 1 )
        day03_lst = db.Get_BetweenDayList( 3 )
        day05_lst = db.Get_BetweenDayList( 5 )
        day10_lst = db.Get_BetweenDayList( 10 )
        day20_lst = db.Get_BetweenDayList( 20 )
        day60_lst = db.Get_BetweenDayList( 60 )

        for val in range( days ):

            df_01 = None
            df_03 = None
            df_05 = None
            df_10 = None
            df_20 = None
            df_60 = None

            try:
                df_01 = db.GetConcentrateBetweenDay( stock, day01_lst[ val ][ 0 ], day01_lst[ val ][ 1 ] )
                df_03 = db.GetConcentrateBetweenDay( stock, day03_lst[ val ][ 0 ], day03_lst[ val ][ 1 ] )
                df_05 = db.GetConcentrateBetweenDay( stock, day05_lst[ val ][ 0 ], day05_lst[ val ][ 1 ] )
                df_10 = db.GetConcentrateBetweenDay( stock, day10_lst[ val ][ 0 ], day10_lst[ val ][ 1 ] )
                df_20 = db.GetConcentrateBetweenDay( stock, day20_lst[ val ][ 0 ], day20_lst[ val ][ 1 ] )
                df_60 = db.GetConcentrateBetweenDay( stock, day60_lst[ val ][ 0 ], day60_lst[ val ][ 1 ]  )
            except IndexError:
                print( stock, '日期範圍不足')

            row = ( stock, day01_lst[ val ][ 0 ], df_01, df_03, df_05, df_10, df_20, df_60 )

            db.Write( row )

            with open( tmp, 'a', newline = '\n', encoding = 'utf8' ) as csv_file:
                file = csv.writer( csv_file, delimiter = ',' )
                file.writerow( row )

    result = pd.read_csv( tmp, sep = ',', encoding = 'utf8', false_values = 'NA',
                          names = cols, dtype={ '股號': str } )

    result.drop_duplicates( [ '日期', '股號' ], keep = 'last', inplace = True )

    result.sort_values( by = [ '日期', '股號' ], ascending = [ False, True ], inplace =  True )

    result = result.reset_index( drop = True )

    df_writer = pd.ExcelWriter( tar_file )
    result.to_excel( df_writer, sheet_name = '籌碼分析' )

    os.remove( tmp )

    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':

    tmr = datetime.now( ).strftime( '%y%m%d_%H%M' )

    unit( '籌碼集中_' + tmr + '.xlsx' )