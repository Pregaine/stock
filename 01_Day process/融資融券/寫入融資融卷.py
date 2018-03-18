# -*- coding: utf-8 -*-

import pyodbc
import re
import os
import csv
from datetime import datetime
import pandas as pd
import time
import numpy as np

class DB_MarginTrad:

    def __init__( self, server, database, username, password ):

        cmd = """SET LANGUAGE us_english; set dateformat ymd;"""
        self.df = pd.DataFrame( )
        self.src_df = pd.DataFrame( )
        self.stock = ''
        self.table = 'MARGIN'

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

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''DROP TABLE IF EXISTS MARGIN;''' ):
            print( 'Successfuly Deleter MARGIN Table' )

    def CreatDB(self):

        with self.cur_db.execute( '''
        
            CREATE TABLE dbo.MARGIN
        	(
                stock varchar( 10 ) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
                date date NOT NULL,

                Financing_Buy        decimal(10, 0) NULL,
                Financing_Sell        decimal(10, 0) NULL,
                Financing_PayOff    decimal(10, 0) NULL,
                Financing_Over       decimal(10, 0) NULL,
                Financing_Increase  decimal(10, 0) NULL,
                Financing_Limit      decimal(10, 0) NULL,
                Financing_Use        decimal(10, 2) NULL,
                
                Margin_Sell           decimal(10, 0) NULL,
                Margin_Buy           decimal(10, 0) NULL,
                Margin_PayOff       decimal(10, 0) NULL,
                Margin_Over          decimal(10, 0) NULL,
                Margin_Increase     decimal(10, 0) NULL,
                Margin_Ratio         decimal(10, 2) NULL,
                Margin_Offset        decimal(10, 0) NULL,
                              
        	)  ON [PRIMARY]

            COMMIT''' ):
            print( 'Successfuly Create 融資融券' )


    def CompareDB( self ):

        cmd = 'SELECT date,  Financing_Buy FROM MARGIN WHERE stock = \'{}\''.format( self.stock )

        ft = self.cur_db.execute( cmd ).fetchall( )

        lst = [ ]

        for val in ft:

            date = val[ 0 ].strftime( '%y%m%d' )

            Financing_Buy = val[ 1 ]

            lst.append( ( date, Financing_Buy ) )

        df_db = pd.DataFrame( lst, columns = [ '日期', 'Financing_Buy_FromDB' ] )

        left = pd.merge( self.df, df_db, on = [ '日期' ], how = 'left' )

        left = left[ left[ 'Financing_Buy_FromDB' ] != left[ '融資買進' ] ]

        del left[ 'Financing_Buy_FromDB' ]

        self.df = left

        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( stock_num, self.df.iloc[ 0 ] )

    def FindDuplicate( self, date ):

        cmd = 'SELECT * FROM {} WHERE stock = \'{}\' and date = \'{}\''.format( self.table, self.stock, date )

        # print( cmd )
        # 尋找重覆資料
        ft = self.cur_db.execute( cmd ).fetchone( )

        print( '比對 {} Table 資料'.format( self.table ) )
        # print( ft )

        if ft is not None:
            cmd = 'DELETE FROM {} WHERE stock = \'{}\' and date = \'{}\''.format( self.table, self.stock, date )
            with self.cur_db.execute( cmd ):
                print( '刪除重覆資料 {} {}'.format( self.stock, date ) )

    def ReadCSV( self, path ):

        self.df = pd.read_csv( path, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )
        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )
        # print( self.df )

    def WriteDB( self, First_Create = False ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( '{} 資料表比對CSV無新資料 {}'.format( self.table, self.stock ) )
            return

        for val in lst:

            val[ 0 ] = self.stock
            dt = datetime.strptime( val[ 1 ], '%y%m%d' )
            val[ 1 ] = dt.strftime( "%y-%m-%d" )

            var_string = ', '.join( '?' * ( len( val )  ) )
            query_string = 'INSERT INTO {} VALUES ( {} );'.format( self.table, var_string )

            print( '取出{}'.format( val ) )

            if First_Create is False:
                self.FindDuplicate( val[ 1 ] )

            with self.cur_db.execute( query_string, val ):
                print( '寫入 {} Table {} {}'.format( self.table, val[ 0 ], val[ 1 ] ) )

def main( ):

    First_Create = False
    try:
        db = DB_MarginTrad( 'localhost', 'StockDB', 'sa', '292929' )
    except Exception:
        db = DB_MarginTrad( 'localhost', 'StockDB', 'sa', 'admin' )

    # First_Create = True
    # db.Reset_Table( )
    # db.CreatDB( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        # 0050_融資融卷
        # if file.endswith( "0050_融資融卷.csv" ) != 1:
        if file.endswith( "_融資融卷.csv" ) != 1:
            continue

        db.stock = file.replace( "_融資融卷.csv", '' )
        print( 'stock {} file {}'.format( db.stock, file ) )

        db.ReadCSV( file )
        db.CompareDB( )
        db.WriteDB( First_Create )
        db.cur_db.commit( )


if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( 'The script took {:06.1f} minute !'.format( (time.time( ) - start_tmr) / 60 ) )