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
from collections import namedtuple
import goodinfo.capital as goodinfo

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
        self.con_db = pyodbc.connect(
            'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )
        self.cur_db.execute( cmd )

    def ResetTable( self, table ):

        cmd = 'DROP TABLE IF EXISTS ' + table

        with self.cur_db.execute( cmd ):
            print( 'Successfully Deleter ' + table )

    def CreateTable( self ):

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
        date = row[ 1 ]

        cmd = 'SELECT * FROM CONCENTRATION WHERE date =  \'{}\'  and stock = \'{}\' '.format( date, stock )
        ft = self.cur_db.execute( cmd ).fetchone( )

        if ft is None:
            var_string = ', '.join( '?' * (len( row )) )
            cmd = 'INSERT INTO CONCENTRATION VALUES ( {} )'.format( var_string )

            with self.cur_db.execute( cmd, row ):
                print( '寫入資料庫', row )

            self.con_db.commit( )

    def GetDates( self, num, days ):

        cmd = 'SELECT TOP ( {0} ) date FROM BROKERAGE WHERE stock = \'{1}\' ' \
              'GROUP BY date ORDER BY date DESC'.format( days, num )

        ft = self.cur_db.execute( cmd ).fetchall( )
        date = [ elt[ 0 ] for elt in ft ]
        self.date_lst = [ val.strftime( "%Y/%m/%d" ) for val in sorted( date, reverse = True ) ]

        # print( '日期範圍 self.date_lst =>', self.date_lst )

    def GetPriceDay( self, symbol, start ):

        cmd = '''SELECT close_price FROM TECH_D WHERE stock = \'{0}\' AND date = \'{1}\' '''.format( symbol, start )

        ft = self.cur_db.execute( cmd ).fetchone( )

        try:
            return round( ft[ 0 ], 1 )
        except Exception:
            print( cmd )
            print( '{0:<7}{1} 無成交量'.format( symbol, start ) )
            return None

    def GetVolumeBetweenDay( self, symbol, start, end ):

        cmd = '''SELECT sum(  CONVERT(  bigint, volume  )  ) as volume FROM TECH_D 
        WHERE stock = \'{0}\' AND date BETWEEN \'{1}\' and \'{2}\' GROUP BY stock'''.format( symbol, start, end )

        ft = self.cur_db.execute( cmd ).fetchall( )

        try:
            return ft[ 0 ][ 0 ]
        except Exception:
            print( cmd )
            print( '{0:<7}{1}~{2} 無成交量'.format( symbol, start, end ) )
            return None

    def Get_BetweenDayList( self, interval ):

        # copy list from self
        cpy_lst = self.date_lst[ : ]
        ret_list = [ ]

        while len( cpy_lst ) > interval:
            ret_list.append( (cpy_lst[ 0 ], cpy_lst[ interval - 1 ]) )
            cpy_lst.pop( 0 )
        return ret_list

    def GetTopBuyBetweenDay( self, symbol, start_day, end_day ):

        chip = namedtuple( 'buy', [ 'brokerage', 'name', 'price', 'vol' ] )
        chip_15 = [ ]
        vol = 0

        cmd = '''SELECT TOP( 15 )
                brokerage,
                brokerage_name,
                sum( buy_volume * price ) -  sum( sell_volume * price ) as buy_sell_price,
                sum( buy_volume - sell_volume ) / 1000 as buy_sell_vol
                FROM BROKERAGE
                WHERE stock = \'{0}\' AND (  date between \'{1}\' and \'{2}\' )
                GROUP BY stock, brokerage, brokerage_name
                ORDER BY buy_sell_vol DESC'''.format( symbol, start_day, end_day )

        try:
            ft = self.cur_db.execute( cmd ).fetchall( )
            for val in map( chip._make, ft ):
                chip_15.append( val )
                vol = vol + val.vol
            return chip_15, vol
        except Exception:
            print( 'Error: {}'.format( cmd ) )
            return None, None  # except Exception:  #     print( cmd )  #     print( '{0:<7} 15大買超無資料 {1}~{2}'.format( symbol, start_day, end_day ) )  #     return None

    def GetTopSellBetweenDay( self, symbol, start_day, end_day ):

        chip = namedtuple( 'sell', [ 'brokerage', 'name', 'price', 'vol' ] )
        chip_15 = [ ]
        vol = 0

        cmd = '''SELECT TOP( 15 )
                brokerage,
                brokerage_name,   	
                sum( sell_volume * price ) - sum( buy_volume * price ) as buy_sell_price,
                sum( sell_volume - buy_volume ) / 1000 as buy_sell_vol                 
                FROM BROKERAGE	
                WHERE stock = \'{0}\' and date between \'{1}\' and \'{2}\'
                GROUP BY stock, brokerage, brokerage_name
                ORDER BY buy_sell_vol DESC'''.format( symbol, start_day, end_day )

        ft = self.cur_db.execute( cmd ).fetchall( )
        try:
            ft = self.cur_db.execute( cmd ).fetchall( )
            for val in map( chip._make, ft ):
                chip_15.append( val )
                vol += val.vol
            return chip_15, vol
        except Exception:
            print( 'Error {}'.format( cmd ) )
            return None, None

    def GetChipSubBetweeDay( self, symbol, start, end ):

        cmd = '''SELECT
                        sum( buy_volume ),
                        sum( sell_volume )
                        FROM BROKERAGE 
                        WHERE stock =\'{}\' AND date BETWEEN \'{}\' and \'{}\'
                        GROUP BY brokerage_name'''
        sql_cal_chip_cmd = cmd.format( symbol, start, end )
        data = self.cur_db.execute( sql_cal_chip_cmd ).fetchall( )
        buy_chip = 0
        sell_chip = 0
        for val in data:
            if val[ 0 ] > 0: buy_chip += 1
            if val[ 1 ] > 0: sell_chip += 1

        try:
            return round( buy_chip - sell_chip, 0 )
        except Exception:
            print( cmd )
            print( '{0:<7}無買賣家數'.format( symbol ) )
            return None

    def GetConcentrateBetweenDay( self, symbol, end, start, val, cap ):

        chip = namedtuple( 'Info', [ 'concentrate', 'sum_vol', 'sub_vol', 'top15_buy', 'top15_sell'
                                    'weight', 'turnover', 'sub_security' ] )

        chip.concentrate = None
        chip.sub_vol     = None
        chip.sum_vol     = None
        chip.top15_buy   = None
        chip.top15_sell  = None
        chip.weight = None
        chip.turnover = None
        chip.sub_security = None

        if len( end ) <= val: return chip

        end = end[ val ][ 0 ]
        start = start[ val ][ 1 ]
        buy_chip, buy_vol = self.GetTopBuyBetweenDay( symbol, start, end )
        sell_chip, sell_vol = self.GetTopSellBetweenDay( symbol, start, end )
        sum_vol = self.GetVolumeBetweenDay( symbol, start, end )
        sub_security = self.GetChipSubBetweeDay( symbol, start, end )

        if buy_vol is None or sell_vol is None or sum_vol is None: return chip
        if sum_vol == 0 or ( buy_vol - sell_vol ) == 0: return chip

        chip.concentrate = round( ( ( buy_vol - sell_vol ) / sum_vol ) * 100, 1 )
        chip.sum_vol = round( float( sum_vol ), 1 )
        chip.sub_vol = round( float( buy_vol - sell_vol ), 0 )
        chip.top15_sell = sell_chip
        chip.top15_buy  = buy_chip
        chip.sub_security = sub_security

        if chip.sub_vol is None: chip.weight = None
        else: chip.weight = round( chip.sub_vol / cap * 100, 2 )

        if chip.sum_vol is None: chip.turnover = None
        else: chip.turnover = round( chip.sum_vol / cap * 100, 2 )

        return chip

def chip_sort( df, stock, date, close_price, obj, day_range, sum_vol ):

    if obj is None or df is None:
        return df

    data = dict( )
    data[ '股號' ] = stock
    data[ '收盤' ] = close_price
    data[ '統計天數' ] = day_range
    data[ '日期' ] = date

    for i in range( 15 ):
        # print( obj[i].price, obj[i].vol, stock, date, day_range, obj[ i ].name )
        data[ '券商{}'.format( i + 1 ) ] = obj[ i ].name
        data[ '券商買賣超{}'.format( i + 1 ) ] = round( obj[ i ].vol, 1 )
        data[ '券商損益{}(萬)'.format( i + 1 ) ] = round( ( ( close_price * obj[ i ].vol ) - ( obj[ i ].price / 1000 ) ) / 10, 1 )
        try:
            data[ '券商均價{}'.format( i + 1 ) ] = round( obj[ i ].price / obj[ i ].vol / 1000, 1 )
        except Exception:
            data[ '券商均價{}'.format( i + 1 ) ] = None

        data[ '重押比{}'.format( i + 1 ) ] = round( float( obj[ i ].vol ) / sum_vol * 100, 1 )

    df = df.append( data, ignore_index = True )
    return df

def unit( tar_file, stock_lst, capital_df ):

    # 回推天數，計算集中度
    days = 5
    tmp  = '籌碼集中暫存.csv'

    try:
        db = dbHandle( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        db = dbHandle( 'localhost', 'StockDB', 'sa', '292929' )

    # db.ResetTable( 'CONCENTRATION' )
    # db.CreateTable(  )
    chip_cols = [ '股號', '日期', '統計天數', '收盤',
                  '券商1', '券商2', '券商3', '券商4', '券商5', '券商6',
                  '券商均價1', '券商均價2', '券商均價3', '券商均價4', '券商均價5', '券商均價6',
                  '券商損益1(萬)', '券商損益2(萬)', '券商損益3(萬)', '券商損益4(萬)', '券商損益5(萬)', '券商損益6(萬)',
                  '重押比1', '重押比2', '重押比3', '重押比4', '重押比5', '重押比6',
                  '券商買賣超1', '券商買賣超2', '券商買賣超3', '券商買賣超4', '券商買賣超5', '券商買賣超6' ]

    if os.path.isfile( 'ChipBuy.csv' ) is False:
        with open( 'ChipBuy.csv', 'w', newline = '\n', encoding = 'utf8' ) as f:
            w = csv.writer( f )
        ChipBuy_df = pd.DataFrame( columns = chip_cols )
    else:
        ChipBuy_df = pd.read_csv( 'ChipBuy.csv', sep = ',', encoding = 'utf8', false_values = 'NA',
        dtype = { '股號': str, '日期': str },  )
        del ChipBuy_df[ 'Unnamed: 0' ]

    if os.path.isfile( 'ChipSell.csv' ) is False:
        with open( 'ChipSell.csv', 'w', newline = '\n', encoding = 'utf8' ) as f:
            w = csv.writer( f )
        ChipSell_df = pd.DataFrame( columns = chip_cols )
    else:
        ChipSell_df = pd.read_csv( 'ChipSell.csv', sep = ',', encoding = 'utf8', false_values = 'NA',
        dtype = { '股號': str, '日期': str } )
        del ChipSell_df[ 'Unnamed: 0' ]

    cols = [ '股號', '日期', '收盤',
             '01天集中%', '03天集中%', '05天集中%', '10天集中%', '20天集中%', '60天集中%',
             '01天佔股本比重', '03天佔股本比重', '05天佔股本比重', '10天佔股本比重', '20天佔股本比重', '60天佔股本比重',
             '01天周轉率', '03天周轉率', '05天周轉率', '10天周轉率', '20天周轉率', '60天周轉率',
             '01天主力買賣超(張)', '03天主力買賣超(張)', '05天主力買賣超(張)', '10天主力買賣超(張)', '20天主力買賣超(張)', '60天主力買賣超(張)',
             '01天家數差', '03天家數差', '05天家數差', '10天家數差', '20天家數差', '60天家數差',
             ]

    if os.path.isfile( tmp ) is False:
        with open( tmp, 'wb' ) as f:
            csv.writer( f )
        df = pd.DataFrame( columns = cols )
    else:
        df = pd.read_csv( tmp, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '股號': str, '日期': str } )
        del df[ 'Unnamed: 0' ]

    while stock_lst:

        stock = stock_lst.pop( 0 )
        print( '{0:<7} {1}'.format( stock, len( stock_lst ) ) )

        db.GetDates( stock, '61' )
        day01_lst = db.Get_BetweenDayList( 1 )
        day03_lst = db.Get_BetweenDayList( 3 )
        day05_lst = db.Get_BetweenDayList( 5 )
        day10_lst = db.Get_BetweenDayList( 10 )
        day20_lst = db.Get_BetweenDayList( 20 )
        day60_lst = db.Get_BetweenDayList( 60 )

        capital = capital_df.loc[ capital_df[ 'stock' ] == stock, '股本(億)' ].values[ 0 ] * 100000000 / 10 / 1000

        for val in range( days ):

            if len( day01_lst ) <= val:
                continue

            codi = ( df['股號'] == stock ) & ( df[ '日期' ] == day01_lst[ val ][ 0 ] )
            if df[ codi ].empty is False:
                continue

            # 買賣超 = 所有券商總買進張數 - 所有券商總賣出張數
            # 家數差=所有買超券商家數-所有賣超券商家數

            chip_01 = db.GetConcentrateBetweenDay( stock, day01_lst, day01_lst, val, capital )
            chip_03 = db.GetConcentrateBetweenDay( stock, day03_lst, day03_lst, val, capital )
            chip_05 = db.GetConcentrateBetweenDay( stock, day05_lst, day05_lst, val, capital )
            chip_10 = db.GetConcentrateBetweenDay( stock, day10_lst, day10_lst, val, capital )
            chip_20 = db.GetConcentrateBetweenDay( stock, day20_lst, day20_lst, val, capital )
            chip_60 = db.GetConcentrateBetweenDay( stock, day60_lst, day60_lst, val, capital )

            date  = day01_lst[ val ][ 0 ]
            price = db.GetPriceDay( stock, date )
            # print( stock, date, chip_01.sub_vol, chip_01.sub_security )

            df = df.append( { '股號': stock,
                              '日期': date,
                              '收盤': price,

                              '01天集中%': chip_01.concentrate,
                              '03天集中%': chip_03.concentrate,
                              '05天集中%': chip_05.concentrate,
                              '10天集中%': chip_10.concentrate,
                              '20天集中%': chip_20.concentrate,
                              '60天集中%': chip_60.concentrate,

                              '01天佔股本比重': chip_01.weight,
                              '03天佔股本比重': chip_03.weight,
                              '05天佔股本比重': chip_05.weight,
                              '10天佔股本比重': chip_10.weight,
                              '20天佔股本比重': chip_20.weight,
                              '60天佔股本比重': chip_60.weight,

                              '01天周轉率': chip_01.turnover,
                              '03天周轉率': chip_03.turnover,
                              '05天周轉率': chip_05.turnover,
                              '10天周轉率': chip_10.turnover,
                              '20天周轉率': chip_20.turnover,
                              '60天周轉率': chip_60.turnover,

                              '01天主力買賣超(張)': chip_01.sub_vol,
                              '03天主力買賣超(張)': chip_03.sub_vol,
                              '05天主力買賣超(張)': chip_05.sub_vol,
                              '10天主力買賣超(張)': chip_10.sub_vol,
                              '20天主力買賣超(張)': chip_20.sub_vol,
                              '60天主力買賣超(張)': chip_60.sub_vol,

                              '01天家數差' : chip_01.sub_security,
                              '03天家數差': chip_03.sub_security,
                              '05天家數差': chip_05.sub_security,
                              '10天家數差': chip_10.sub_security,
                              '20天家數差': chip_20.sub_security,
                              '60天家數差': chip_60.sub_security,

                              }, ignore_index=True )

            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_01.top15_buy, 1, chip_01.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_03.top15_buy, 3, chip_03.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_05.top15_buy, 5, chip_05.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_10.top15_buy, 10, chip_10.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_20.top15_buy, 20, chip_20.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_60.top15_buy, 60, chip_60.sum_vol )

            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_01.top15_sell, 1, chip_01.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_03.top15_sell, 3, chip_03.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_05.top15_sell, 5, chip_05.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_10.top15_sell, 10, chip_10.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_20.top15_sell, 20, chip_20.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_60.top15_sell, 60, chip_60.sum_vol )

    df.sort_values( by = [ '日期', '股號' ], ascending = [ False, True ], inplace = True )

    df_writer = pd.ExcelWriter( tar_file )

    df.reset_index( drop = True, inplace = True )
    ChipBuy_df.reset_index( drop = True, inplace =  True )
    ChipSell_df.reset_index( drop = True, inplace =  True )

    df.to_csv( tmp, encoding = 'utf-8' )
    ChipBuy_df.to_csv( 'ChipBuy.csv', encoding = 'utf-8' )
    ChipSell_df.to_csv( 'ChipSell.csv', encoding = 'utf-8' )

    df.to_excel( df_writer, sheet_name = '籌碼分析' )
    ChipBuy_df.to_excel( df_writer, sheet_name = '卷商買超' )
    ChipSell_df.to_excel( df_writer, sheet_name = '卷商賣超' )


if __name__ == '__main__':

    start_tmr = datetime.now( )
    # stock_lst = list( TWSE.codes.keys( ) )
    # stock_lst = sorted( stock_lst )

    capital = goodinfo.capital( path = 'C:\workspace\stock\goodinfo\StockList_股本.csv' )
    condition = capital.df[ '股本(億)' ] > 20
    capital.df = capital.df[ condition ]

    stock_lst = sorted( capital.df[ 'stock' ].tolist( ) )
    # stock_lst = [ '2330' ]

    tmr = datetime.now( ).strftime( '%y%m%d_%H%M' )
    unit( '籌碼集中_{}.xlsx'.format( tmr ), stock_lst, capital.df )

    print( datetime.now( ) - start_tmr )

