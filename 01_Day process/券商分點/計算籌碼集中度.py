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
import codes.codes as TWSE
import decimal

# Todo
# 建立 Table
# date, 股號, top 15 買均價 top 15賣均價 top 15 買進金額 top 15賣出金額
# 1日 5日 10日 20日 30日 45日 60日 120日

class dbHandle:

    def __init__( self, server, database, username, password ):

        cmd = """SET LANGUAGE us_english; set dateformat ymd;"""

        self.date_lst = [ ]
        print( "Initial Database connection..." + database )

        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID='  + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )
        self.cur_db.execute( cmd )

    def ResetTable(self, table ):

       cmd = 'DROP TABLE IF EXISTS ' + table

       with self.cur_db.execute( cmd ):
            print( 'Successfully Deleter ' + table )

    def CreateTable(self):

        cmd = '''CREATE TABLE dbo.CONCENTRATION
                        (
                        stock nvarchar(10) NOT NULL,
                        date date NOT NULL,
                        one decimal(6, 2) NULL,
                        three decimal(6, 2) NULL,
                        five decimal(6, 2) NULL,
                        ten decimal(6, 2) NULL,
                        twenty decimal(6, 2) NULL,
                        sixty decimal(6, 2) NULL
                        )  ON [PRIMARY]
                        
                        COMMIT'''

        self.cur_db.execute( cmd )

    def Write( self, row ):

        stock = row[ 0 ]
        date  = row[ 1 ]

        cmd = 'SELECT * FROM CONCENTRATION WHERE date =  \'{}\'  and stock = \'{}\' '.format( date, stock )
        ft = self.cur_db.execute( cmd ).fetchone( )

        if ft is None:
            var_string = ', '.join( '?' * ( len( row ) ) )
            cmd = 'INSERT INTO CONCENTRATION VALUES ( {} )'.format( var_string )

            with self.cur_db.execute( cmd, row ):
                print( '寫入資料庫', row  )

            self.con_db.commit( )

    def GetDates( self, num, days ):

        cmd = 'SELECT TOP ( {0} ) date FROM BROKERAGE WHERE stock = \'{1}\' ' \
              'GROUP BY date ORDER BY date DESC'.format( days, num )

        ft = self.cur_db.execute( cmd ).fetchall( )
        date = [ elt[ 0 ] for elt in ft ]
        self.date_lst = [ val.strftime( "%Y/%m/%d" ) for val in sorted( date, reverse = True ) ]

        # print( '日期範圍 self.date_lst =>', self.date_lst )

    def GetVolumeBetweenDay( self, symbol, start, end ):

        cmd = ''' SELECT sum(  CONVERT(  bigint, volume  )  ) as volume FROM TECH_D
                        WHERE stock = \'{0}\' AND date BETWEEN \'{1}\' and \'{2}\' GROUP BY stock'''.format( symbol, start, end )

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

    def GetTopBuyBetweenDay(self, symbol, start_day, end_day ):

        # buy top 15
        cmd = ''' SELECT TOP( 15 )
                sum( buy_volume * price ) -  sum( sell_volume * price ) as buy_sell_price,
                sum( buy_volume - sell_volume ) / 1000 as buy_sell_vol
                FROM BROKERAGE
                WHERE stock = \'{0}\' AND (  date between \'{1}\' and \'{2}\' )
                GROUP BY stock, brokerage
                ORDER BY buy_sell_price DESC'''.format( symbol, start_day, end_day )

        ft = self.cur_db.execute( cmd ).fetchall( )

        # price = ft[ 0 ][ 0 ]
        vol = 0
        for v in ft:
            vol = vol + v[ 1 ]
        return vol

    def GetTopSellBetweenDay(self, symbol, start_day, end_day ):

        cmd = ''' SELECT TOP( 15 )  	
                sum( sell_volume * price ) - sum( buy_volume * price ) as buy_sell_price,
                sum( sell_volume - buy_volume ) / 1000 as buy_sell_vol                 
                FROM BROKERAGE	
                WHERE stock = \'{0}\' and date between \'{1}\' and \'{2}\'
                GROUP BY stock, brokerage
                ORDER BY buy_sell_price DESC'''.format( symbol, start_day, end_day )

        ft = self.cur_db.execute( cmd ).fetchall( )
        # price = ft[ 0 ][ 0 ]
        vol = 0
        for v in ft:
            vol = vol + v[ 1 ]
        return vol

    def GetConcentrateBetweenDay( self, symbol, end, start ):

        buy_vol  = self.GetTopBuyBetweenDay( symbol, start, end )
        sell_vol = self.GetTopSellBetweenDay( symbol, start, end )
        sum_vol  = self.GetVolumeBetweenDay( symbol, start, end )

        concentrate = None
        if sum_vol is not 0:
            concentrate =  ( ( buy_vol - sell_vol ) / sum_vol ) * 100
            concentrate = concentrate.__round__( 2 )

        # row = '{0}~{1} 買總量 {2:.1f} 賣總量 {3:.1f} 總量 {4:.1f} 集中度 {5:.2f}'.format( end, start, buy_vol, sell_vol, sum_vol, concentrate )
        # print( '{0} {1}'.format( symbol, row )  )

        return concentrate

def unit( tar_file ):

    # 回推天數，計算集中度
    days = 20
    tmp = '籌碼集中暫存.csv'

    try:
        db = dbHandle( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        db = dbHandle( 'localhost', 'StockDB', 'sa', '292929' )

    # db.ResetTable( 'CONCENTRATION' )
    # db.CreateTable(  )

    stock_lst = list( TWSE.codes.keys( ) )

    if os.path.isfile( tmp ) is False:
        with open( tmp, 'wb' ) as f:
            csv.writer( f )

    cols = [ '股號', '日期', '01天', '03天', '05天', '10天', '20天', '60天' ]

    df = pd.read_csv( tmp, sep = ',', encoding = 'utf8', false_values = 'NA',
                      names = cols, dtype={ '股號': str, '日期':str } )

    for stock in stock_lst:
    # for stock in [ '2722' ]:

        db.GetDates( stock, '61' )

        day01_lst = db.Get_BetweenDayList( 1 )
        day03_lst = db.Get_BetweenDayList( 3 )
        day05_lst = db.Get_BetweenDayList( 5 )
        day10_lst = db.Get_BetweenDayList( 10 )
        day20_lst = db.Get_BetweenDayList( 20 )
        day60_lst = db.Get_BetweenDayList( 60 )
        # day120_lst = db.Get_BetweenDayList( 120 )

        for val in range( days ):
            print( stock, val, day01_lst )

            if len( day01_lst ) <= val:
                continue

            if df[ '日期' ].str.contains( day01_lst[ val ][ 0 ] ).any( ):
                if df[ '股號' ].str.contains( stock ).any( ):
                    continue

            df_01 = None
            df_03 = None
            df_05 = None
            df_10 = None
            df_20 = None
            df_60 = None
            # df_120 = None

            try:
                df_01 = db.GetConcentrateBetweenDay( stock, day01_lst[ val ][ 0 ], day01_lst[ val ][ 1 ] )
                df_03 = db.GetConcentrateBetweenDay( stock, day03_lst[ val ][ 0 ], day03_lst[ val ][ 1 ] )
                df_05 = db.GetConcentrateBetweenDay( stock, day05_lst[ val ][ 0 ], day05_lst[ val ][ 1 ] )
                df_10 = db.GetConcentrateBetweenDay( stock, day10_lst[ val ][ 0 ], day10_lst[ val ][ 1 ] )
                df_20 = db.GetConcentrateBetweenDay( stock, day20_lst[ val ][ 0 ], day20_lst[ val ][ 1 ] )
                df_60 = db.GetConcentrateBetweenDay( stock, day60_lst[ val ][ 0 ], day60_lst[ val ][ 1 ] )
                # df_120 = db.GetConcentrateBetweenDay( stock, day120_lst[ val ][ 0 ], day120_lst[ val ][ 1 ] )
            except IndexError:
                print( stock, '日期範圍不足' )

            row = ( stock, day01_lst[ val ][ 0 ], df_01, df_03, df_05, df_10, df_20, df_60 )
            # db.Write( row )

            with open( tmp, 'a', newline = '\n', encoding = 'utf8' ) as csv_file:
                file = csv.writer( csv_file, delimiter = ',' )
                file.writerow( row )

    result = pd.read_csv( tmp, sep = ',', encoding = 'utf8', false_values = 'NA', names = cols, dtype={ '股號': str, '日期':str } )

    result.sort_values( by = [ '日期', '股號' ], ascending = [ False, True ], inplace =  True )
    result = result.reset_index( drop = True )
    df_writer = pd.ExcelWriter( tar_file )
    result.to_excel( df_writer, sheet_name = '籌碼分析' )

    # os.remove( tmp )

if __name__ == '__main__':

    start_tmr = datetime.now( )

    tmr = datetime.now( ).strftime( '%y%m%d_%H%M' )
    unit( '籌碼集中_{}.xlsx'.format( tmr ) )

    print( datetime.now( ) - start_tmr )