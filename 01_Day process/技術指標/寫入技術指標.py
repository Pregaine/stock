# -*- coding: utf-8 -*-

import pyodbc
from datetime import datetime
import pandas as pd
import re
import numpy as np
import glob
import time
import urllib
from sqlalchemy import create_engine

def Connection():
    return pyodbc.connect(
    'DRIVER={ODBC Driver 13 for SQL Server};'+
    'SERVER=localhost;' +
    'PORT=1443;' +
    'DATABASE=StockDB;' +
    'UID=sa;'+
    'PWD=admin;' )

# Create the sqlalchemy engine using the pyodbc connection
engine = create_engine( "mssql+pyodbc://", creator = Connection, convert_unicode = True )
con = engine.connect( )
con.execute( "SET LANGUAGE us_english; set dateformat ymd;" )
con.close()

def StrToDateFormat( data, val ):

    # print( 'data {}, val {}'.format( data, val ) )

    if data != '分':
        dt = datetime.strptime( val, '%y%m%d' )
        val = dt.strftime( "%y-%m-%d" )
    else:
        dt = datetime.strptime( val, '%y%m%d%H' )
        val = dt.strftime( "%y-%m-%d %H:%M:%S" )

    return val

class DB_TechAnalysis:

    def __init__( self, server, database, username, password ):

        #  TODO 如何查當下SQL 語言及時間格式

        cmd = """SET LANGUAGE us_english; set dateformat ymd;"""

        self.df = pd.DataFrame( )
        self.src_df = pd.DataFrame( )

        self.d = { '分': 'TECH_H',
                   '日': 'TECH_D',
                   '周': 'TECH_W',
                   '月': 'TECH_M' }

        self.datelst = [ ]
        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.con_db.setencoding( 'utf-8' )
        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )

        self.cur_db.execute(cmd)

        self.stock = ''
        self.date = ''

    def ResetTable( self, data ):

        d = dict( 分 = 'DROP TABLE IF EXISTS TECH_H', 日 = 'DROP TABLE IF EXISTS TECH_D',
                  周 = 'DROP TABLE IF EXISTS TECH_W', 月 = 'DROP TABLE IF EXISTS TECH_M' )

        # Do some setup
        with self.cur_db.execute( d[ data ] ):
            print( 'Successfully Deleter ' + data )

    def CreateTable( self, data ):

        sql_m_cmd = '''
            CREATE TABLE dbo.TECH_M
            (
            stock varchar( 10 ) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
            date date NOT NULL,
            
            open_price decimal(10, 2) NULL,
            high_price decimal(10, 2) NULL,
            low_price decimal(10, 2) NULL,
            close_price decimal(10, 2) NULL,
            volume decimal( 16, 1 ) NULL,
            
            ma3 decimal(10, 2) NULL,
            ma6 decimal(10, 2) NULL,
            ma12 decimal(10, 2) NULL,
            ma24 decimal(10, 2) NULL,
            ma36 decimal(10, 2) NULL,
            ma60 decimal(10, 2) NULL,
            ma120 decimal(10, 2) NULL,
            
            rsi2 decimal(10, 2) NULL,
            rsi5 decimal(10, 2) NULL,
            rsi10 decimal(10, 2) NULL,
            
            k9_3 decimal(10, 2) NULL,
            d9_3 decimal(10, 2) NULL,
            k3_2 decimal(10, 2) NULL,
            d3_3 decimal(10, 2) NULL,
            
            mfi4 decimal(10, 2) NULL,
            mfi6 decimal(10, 2) NULL,
            mfi14 decimal(10, 2) NULL,
            
            macd_dif_6 decimal(10, 2) NULL,
            dem_12 decimal(10, 2) NULL,
            osc_6_12_9 decimal(10, 2) NULL,
            
            macd_dif_12 decimal(10, 2) NULL,
            dem_26 decimal(10, 2) NULL,
            osc6_12_26_9 decimal(10, 2) NULL,
            
            willr9 decimal(10, 2) NULL,
            willr18 decimal(10, 2) NULL,
            willr42 decimal(10, 2) NULL,
            willr14 decimal(10, 2) NULL,
            willr24 decimal(10, 2) NULL,
            willr56 decimal(10, 2) NULL,
            willr72 decimal(10, 2) NULL,
            
            plus_di decimal(10, 2) NULL,
            minus_di decimal(10, 2) NULL,
            dx decimal(10, 2) NULL,
            adx decimal(10, 2) NULL,
            
            upperband decimal(10, 2) NULL,
            middleband decimal(10, 2) NULL,
            dnperband decimal(10, 2) NULL,
            
            bb decimal(10, 2) NULL,
            w20 decimal(10, 2) NULL,
            bias20 decimal(10, 2) NULL,
            bias60 decimal(10, 2) NULL
            
            )  ON [PRIMARY]

        COMMIT'''

        sql_h_cmd = '''
        CREATE TABLE dbo.TECH_H
        (
        stock varchar(10) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
        date smalldatetime NOT NULL,
        open_price decimal(10, 2) NULL,
        high_price decimal(10, 2) NULL,
        low_price decimal(10, 2) NULL,
        close_price decimal(10, 2) NULL,
        volume decimal( 16, 1 ) NULL,
        
        ma25 decimal(10, 2) NULL,
        ma50 decimal(10, 2) NULL,
        ma100 decimal(10, 2) NULL,
        ma300 decimal(10, 2) NULL,
        ma600 decimal(10, 2) NULL,
        ma1200 decimal(10, 2) NULL,
        rsi2 decimal(10, 2) NULL,
        rsi4 decimal(10, 2) NULL,
        rsi5 decimal(10, 2) NULL,
        rsi10 decimal(10, 2) NULL,
        k9_3 decimal(10, 2) NULL,
        d9_3 decimal(10, 2) NULL,
        k3_2 decimal(10, 2) NULL,
        d3_3 decimal(10, 2) NULL,
        mfi5 decimal(10, 2) NULL,
        mfi6 decimal(10, 2) NULL,
        mfi14 decimal(10, 2) NULL,

        macd_dif_6 decimal(10, 2) NULL,
        dem_12 decimal(10, 2) NULL,
        osc_6_12_9 decimal(10, 2) NULL,

        macd_dif_12 decimal(10, 2) NULL,
        dem_26 decimal(10, 2) NULL,
        osc6_12_26_9 decimal(10, 2) NULL,

        willr9 decimal(10, 2) NULL,
        willr18 decimal(10, 2) NULL,
        willr42 decimal(10, 2) NULL,
        willr14 decimal(10, 2) NULL,
        willr24 decimal(10, 2) NULL,
        willr56 decimal(10, 2) NULL,
        willr72 decimal(10, 2) NULL,
        plus_di decimal(10, 2) NULL,
        minus_di decimal(10, 2) NULL,
        dx decimal(10, 2) NULL,
        adx decimal(10, 2) NULL,
        upperband decimal(10, 2) NULL,
        middleband decimal(10, 2) NULL,
        dnperband decimal(10, 2) NULL,
        bb decimal(10, 2) NULL,
        w20 decimal(10, 2) NULL,
        bias20 decimal(10, 2) NULL,
        bias60 decimal(10, 2) NULL
        )  ON [PRIMARY]

        COMMIT'''

        sql_d_cmd = '''
        CREATE TABLE dbo.TECH_D
	    (
	    stock varchar( 10 ) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
	    date date NOT NULL,
	    open_price decimal(10, 2) NULL,
	    high_price decimal(10, 2) NULL,
	    low_price decimal(10, 2) NULL,
	    close_price decimal(10, 2) NULL,
	    volume decimal( 16, 1 ) NULL,
	    
	    d_pec decimal(10, 2) NULL,
	    w_pec decimal(10, 2) NULL,
	    m_pec decimal(10, 2) NULL,
	    ma3 decimal(10, 2) NULL,
	    ma5 decimal(10, 2) NULL,
	    ma8 decimal(10, 2) NULL,
	    ma10 decimal(10, 2) NULL,
	    ma20 decimal(10, 2) NULL,
	    ma60 decimal(10, 2) NULL,
	    ma120 decimal(10, 2) NULL,
	    ma240 decimal(10, 2) NULL,
	    ma480 decimal(10, 2) NULL,
	    ma600 decimal(10, 2) NULL,
	    ma840 decimal(10, 2) NULL,
	    ma1200 decimal(10, 2) NULL,
	    rsi2 decimal(10, 2) NULL,
	    rsi4 decimal(10, 2) NULL,
	    rsi5 decimal(10, 2) NULL,
	    rsi10 decimal(10, 2) NULL,
	    k9_3 decimal(10, 2) NULL,
	    d9_3 decimal(10, 2) NULL,
	    k3_2 decimal(10, 2) NULL,
	    d3_3 decimal(10, 2) NULL,
	    mfi4 decimal(10, 2) NULL,
	    mfi6 decimal(10, 2) NULL,
	    mfi14 decimal(10, 2) NULL,
	    macd_dif_6 decimal(10, 2) NULL,
	    dem_12 decimal(10, 2) NULL,
	    osc_6_12_9 decimal(10, 2) NULL,
	    macd_dif_12 decimal(10, 2) NULL,
	    dem_26 decimal(10, 2) NULL,
	    osc6_12_26_9 decimal(10, 2) NULL,
	    willr9 decimal(10, 2) NULL,
	    willr18 decimal(10, 2) NULL,
	    willr42 decimal(10, 2) NULL,
	    willr14 decimal(10, 2) NULL,
	    willr24 decimal(10, 2) NULL,
	    willr56 decimal(10, 2) NULL,
	    willr72 decimal(10, 2) NULL,
	    plus_di decimal(10, 2) NULL,
	    minus_di decimal(10, 2) NULL,
	    dx decimal(10, 2) NULL,
	    adx decimal(10, 2) NULL,
	    upperband decimal(10, 2) NULL,
	    middleband decimal(10, 2) NULL,
	    dnperband decimal(10, 2) NULL,
	    bb decimal(10, 2) NULL,
	    w20 decimal(10, 2) NULL,
	    bias20 decimal(10, 2) NULL,
	    bias60 decimal(10, 2) NULL
	    )  ON [PRIMARY]

        COMMIT'''

        sql_w_cmd = '''
        CREATE TABLE dbo.TECH_W
        (
        stock varchar( 10 ) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
        date date NOT NULL,
        
        open_price decimal(10, 2) NULL,
        high_price decimal(10, 2) NULL,
        low_price decimal(10, 2) NULL,
        close_price decimal(10, 2) NULL,
        volume bigint NULL,
        
        ma4 decimal(10, 2) NULL,
        ma12 decimal(10, 2) NULL,
        ma24 decimal(10, 2) NULL,
        ma48 decimal(10, 2) NULL,
        ma96 decimal(10, 2) NULL,
        ma144 decimal(10, 2) NULL,
        ma240 decimal(10, 2) NULL,
        ma480 decimal(10, 2) NULL,
        
        rsi2 decimal(10, 2) NULL,
        rsi3 decimal(10, 2) NULL,
        rsi4 decimal(10, 2) NULL,
        rsi5 decimal(10, 2) NULL,
        rsi10 decimal(10, 2) NULL,
        
        k9_3 decimal(10, 2) NULL,
        d9_3 decimal(10, 2) NULL,
        k3_2 decimal(10, 2) NULL,
        d3_3 decimal(10, 2) NULL,
        
        mfi4 decimal(10, 2) NULL,
        mfi6 decimal(10, 2) NULL,
        mfi14 decimal(10, 2) NULL,

        macd_dif_6 decimal(10, 2) NULL,
        dem_12 decimal(10, 2) NULL,
        osc_6_12_9 decimal(10, 2) NULL,

        macd_dif_12 decimal(10, 2) NULL,
        dem_26 decimal(10, 2) NULL,
        osc6_12_26_9 decimal(10, 2) NULL,

        willr9 decimal(10, 2) NULL,
        willr18 decimal(10, 2) NULL,
        willr42 decimal(10, 2) NULL,
        willr14 decimal(10, 2) NULL,
        willr24 decimal(10, 2) NULL,
        willr56 decimal(10, 2) NULL,
        willr72 decimal(10, 2) NULL,
        
        plus_di decimal(10, 2) NULL,
        minus_di decimal(10, 2) NULL,
        dx decimal(10, 2) NULL,
        adx decimal(10, 2) NULL,
        upperband decimal(10, 2) NULL,
        middleband decimal(10, 2) NULL,
        dnperband decimal(10, 2) NULL,
        bb decimal(10, 2) NULL,
        w20 decimal(10, 2) NULL,
        bias20 decimal(10, 2) NULL,
        bias60 decimal(10, 2) NULL
        )  ON [PRIMARY]

        COMMIT'''

        table_d = { '月': sql_m_cmd, '日': sql_d_cmd, '周': sql_w_cmd, '分': sql_h_cmd }

        with self.cur_db.execute( table_d[ data ] ):
            print( 'Successfully Create 技術指標 ' + data )


    def CompareDB( self, data ):
        # print( table_name, stock_num )
        cmd = 'SELECT date, volume FROM {0} WHERE stock = \'{1}\''.format( self.d[ data ], self.stock )
        ft = self.cur_db.execute( cmd ).fetchall( )
        lst = [ ]

        for val in ft:
            if data != '分':
                date = val[ 0 ].strftime( '%y%m%d' )
            else:
                date = val[ 0 ].strftime( '%y%m%d%H' )

            volume = val[ 1 ]
            lst.append( ( date, volume ) )

        df = pd.DataFrame( lst, columns = [ '日期', '成交量_資料庫取出' ] )
        # print( df.head( 5 ) )
        left = pd.merge( self.df, df, on = [ '日期' ], how = 'left' )
        left = left[ left[ '成交量_資料庫取出' ] != left[ '成交量' ]  ]
        del left[ '成交量_資料庫取出' ]
        self.df = left
        #  self.df = self.df[ ~self.df[ '日期' ].isin( lst ) ]
        #  print( data, '刪除重覆寫入', self.df )

    def ReadCSV( self, file ):
        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )
        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )
        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )
        # print( self.df )

    def FindDuplicate( self, data ):

        # 尋找重覆資料
        cmd = '''SELECT stock, date from {} where stock = ? and date = ? '''.format( self.d[ data ] )
        ft = self.cur_db.execute( cmd, ( self.stock, self.date )  ).fetchone( )
        print( '比對資料庫{0:>10} {1}'.format( self.stock, self.date ) )

        if ft is not None:
            cmd = 'DELETE FROM {} where stock = ? and date = ?'.format( self.d[ data ] )
            with self.cur_db.execute( cmd, ( self.stock, self.date ) ):
                print( '刪除重覆資料{0:>7}{1}'.format(self.stock, self.date) )

    def WriteDB( self, data, First_Create = False ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        if self.df.empty:
            print( '{:<7}exist DB'.format(self.stock) )
            return

        del self.df[ 'Unnamed: 0' ]
        self.df.insert( 0, 'stock', self.stock )
        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = '%y%m%d' )
        # self.df[ '日期' ] = self.df[ '日期' ].dt.strftime( "%y-%m-%d" )

        self.df.columns = [ 'stock',
                            'date',
                            'open_price',
                            'high_price',
                            'low_price',
                            'close_price',
                            'volume',
                            'd_pec',
                            'w_pec',
                            'm_pec',
                            'ma3',
                            'ma5',
                            'ma8',
                            'ma10',
                            'ma20',
                            'ma60',
                            'ma120',
                            'ma240',
                            'ma480',
                            'ma600',
                            'ma840',
                            'ma1200',
                            'rsi2',
                            'rsi4',
                            'rsi5',
                            'rsi10',
                            'k9_3',
                            'd9_3',
                            'k3_2',
                            'd3_3',
                            'mfi4',
                            'mfi6',
                            'mfi14',
                            'macd_dif_6',
                            'dem_12',
                            'osc_6_12_9',
                            'macd_dif_12',
                            'dem_26',
                            'osc6_12_26_9',
                            'willr9',
                            'willr18',
                            'willr42',
                            'willr14',
                            'willr24',
                            'willr56',
                            'willr72',
                            'plus_di',
                            'minus_di',
                            'dx',
                            'adx',
                            'upperband',
                            'middleband',
                            'dnperband',
                            'bb',
                            'w20',
                            'bias20',
                            'bias60' ]

        # Try to send it to the access database (and fail)
        self.df.to_sql( name = self.d[ data ], con = engine, index=False, if_exists = 'append', index_label = None )
        print( '寫入資料庫{0:>2}{1:>7} {2}'.format( data, self.stock, self.date ) )

def main( ):

    First_Create = False
    try:
        db_M = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', 'admin' )
        db_W = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', 'admin' )
        db_D = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', 'admin' )
        db_H = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        print( '{}'.format( e ) )
        db_M = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', '292929' )
        db_W = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', '292929' )
        db_D = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', '292929' )
        db_H = DB_TechAnalysis( 'localhost', 'StockDB', 'sa', '292929' )

    #  移除表格
    # First_Create = True
    # db_M.ResetTable( '月' )
    # db_W.ResetTable( '周' )
    # db_D.ResetTable( '日' )
    # db_H.ResetTable( '分' )

    #  建立資料表
    # db_M.CreateTable( '月' )
    # db_W.CreateTable( '周' )
    # db_D.CreateTable( '日' )
    # db_H.CreateTable( '分' )

    stock_d = {
            '分': [ db_H, '_60分線技術指標.csv'],
            '日': [ db_D, '_日線技術指標.csv'],
            '周': [ db_W, '_周線技術指標.csv'],
            '月': [ db_M, '_月線技術指標.csv'] }

    # 讀取資料夾
    for file in glob.glob( '*_日線技術指標.csv' ):
        # num = re.match( '\d*', file ).group( 0 )
        # print( file.split( '_', 1 )[ 0 ], file )
        num = file.split( '_' )[ 0 ]
        data = file[ -10:-9 ]

        if data in stock_d.keys( ):
            Obj = stock_d[ data ][ 0 ]
            path = num + stock_d[ data ][ 1 ]
            Obj.stock = num
            print( '讀取{}'.format( path ) )
            Obj.ReadCSV( path )
            Obj.CompareDB( data )
            Obj.WriteDB( data, First_Create )
        else:
            print( '讀取錯誤 {}'.format( data ) )

if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( '{:04.1f}'.format( ( time.time( ) - start_tmr ) ) )