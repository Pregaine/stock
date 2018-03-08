# -*- coding: utf-8 -*-

import pyodbc
import os
import csv
from datetime import datetime
import pandas as pd
import numpy as np
import time

class DB_Investors :

    def __init__( self, server, database, username, password ):

        cmd = """SET LANGUAGE us_english; set dateformat ymd;"""

        self.df = pd.DataFrame( )
        self.src_df = pd.DataFrame( )

        self.datelst = [ ]
        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )
        self.cur_db.execute( cmd )
        self.stock = ''

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''DROP TABLE IF EXISTS INVESTORS;''' ):
            print( 'Successfuly Deleter INVESTORS' )

    def CreatDB( self ):

        with self.cur_db.execute( '''

            CREATE TABLE dbo.INVESTORS 
        	(
                stock int NOT NULL,
                date date NOT NULL,
                
                foreign_sell int,
                investment_sell int,
                dealer_sell int,
                single_day_sell int, 
                 
                foreign_estimate int,
                investment_estimate int,
                dealer_estimate int,
                single_day_estimate int,
                
                foreign_ratio float,
                investment_ratio float
                
        	)  ON [PRIMARY]

            COMMIT''' ):
            print( 'Successfuly Create 3大法人' )

    def CompareDB( self ):

        cmd = 'select date, foreign_sell from INVESTORS where stock = {}'.format( self.stock )

        ft = self.cur_db.execute( cmd ).fetchall( )

        lst = [ ]

        for val in ft:

            date = val[ 0 ].strftime( '%y%m%d' )

            foreign_sell = val[ 1 ]

            lst.append( ( date, foreign_sell ) )

        df_db = pd.DataFrame( lst, columns = [ '日期', 'foreign_sell_FromDB' ] )

        left = pd.merge( self.df, df_db, on = [ '日期' ], how = 'left' )

        left = left[ left[ 'foreign_sell_FromDB' ] != left[ '外資買賣超' ] ]

        del left[ 'foreign_sell_FromDB' ]

        self.df = left
        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( stock_num, self.df.iloc[ 0 ] )

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )

        #  self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )
        #  print( self.df )

    def WriteDB( self ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( '資料庫比對CSV無新資料 {}'.format( self.stock ) )
            return

        for val in lst:

            val[ 0 ] = self.stock
            dt = datetime.strptime( val[ 1 ], '%y%m%d' )
            val[ 1 ] = dt.strftime( "%y-%m-%d" )
            var_string = ', '.join( '?' * ( len( val )  ) )

            query_string = 'INSERT INTO INVESTORS VALUES ( {} );'.format( var_string )

            print( '取出{}'.format( val ) )

            with self.cur_db.execute( query_string, val ):
                print( '寫入資料庫 {} {}'.format( val[ 0 ], val[ 1 ] ) )

def main( ):

    try:
        db = DB_Investors( 'localhost', 'StockDB', 'sa', 'admin' )
    except:
        db = DB_Investors( 'localhost', 'StockDB', 'sa', '292929' )

    db.Reset_Table( )
    db.CreatDB( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        db.stock = file[ 0:4 ]

        db.ReadCSV( file )

        db.CompareDB( )

        db.WriteDB( )

        db.cur_db.commit( )

if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( 'The script took {:06.1f} minute !'.format( (time.time( ) - start_tmr) / 60 ) )