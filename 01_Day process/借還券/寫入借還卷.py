# -*- coding: utf-8 -*-

import pyodbc
import os
from datetime import datetime
import pandas as pd
import time


class DB_Lend:

    def __init__( self, server, database, username, password ):

        cmd = """SET LANGUAGE us_english; set dateformat ymd;"""
        self.df = pd.DataFrame( )
        self.src_df = pd.DataFrame( )

        self.date = ''
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
        with self.cur_db.execute( '''DROP TABLE IF EXISTS LEND;''' ):
            print( 'Successfuly Deleter all Table' )

    def CreatDB( self ):

        with self.cur_db.execute( '''

            CREATE TABLE dbo.LEND 
        	(
                stock int NOT NULL,
                date date NOT NULL,
                
                lend_over int,
                lend_chang int,
                lend_return int,
                lend_diff int,
                
                sell_over int,
                sell_day int,
                sell_day_return int,
                sell_day_diff int,
                sell_day_limit int                

        	)  ON [PRIMARY]

            COMMIT''' ):
            print( 'Successfuly Create 借還券' )

    def FindDuplicate( self, num, date ):

        cmd = '''SELECT * FROM LEND WHERE stock = {} and date = \'{}\' '''.format( num, date )

        # 尋找重覆資料
        ft = self.cur_db.execute( cmd  ).fetchone( )

        print( '比對資料庫資料 {} {}'.format( num, date ) )

        if ft is not None:
            cmd = 'DELETE FROM LEND WHERE stock = {} and date = \'{}\''.format( num, date )

            with self.cur_db.execute( cmd ):
                print( '刪除重覆資料 {} {}'.format( num, date ) )

    def CompareDB( self ):

        cmd = 'SELECT stock, lend_over FROM LEND WHERE date = \'{}\''.format( self.date )
        ft = self.cur_db.execute( cmd ).fetchall( )
        lst = [ ]

        for val in ft:
            stock     = val[ 0 ]
            lend_over = val[ 1 ]
            lst.append( ( stock, lend_over ) )

        df_db = pd.DataFrame( lst, columns = [ '股票代號', 'lend_over_FromDB' ] )

        left = pd.merge( self.df, df_db, on = [ '股票代號' ], how = 'left' )

        left = left[ left[ 'lend_over_FromDB' ] != left[ '借券餘額股' ] ]

        del left[ 'lend_over_FromDB' ]

        self.df = left

        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( self.df.iloc[ 0 ] )

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

        del self.df[ '證券名稱' ]

        mask = ( self.df[ '股票代號' ].str.len( ) == 4 )

        self.df = self.df.loc[ mask ]

        self.df[ "股票代號" ] = self.df[ "股票代號" ].astype( "int" )

        # print( self.df )

    def WriteDB( self, First_Create ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( '資料庫比對CSV無新資料 {}'.format( self.date ) )
            return

        for val in lst:

            val.pop( 0 )
            dt = datetime.strptime( self.date, '%y%m%d' )
            val.insert( 1, dt.strftime( "%y-%m-%d" ) )
            var_string = ', '.join( '?' * (len( val ) ) )

            if First_Create is False:
                self.FindDuplicate( val[ 0 ], val[ 1 ] )

            query_string = 'INSERT INTO LEND VALUES ( {} );'.format( var_string )
            print( '取出 {}'.format( val ) )

            with self.cur_db.execute( query_string, val ):
                print( '寫入資料庫 {} {}'.format( val[ 0 ], val[ 1 ] ) )



def main( ):

    try:
        db = DB_Lend( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        print( '{}'.format( e ) )
        db = DB_Lend( 'localhost', 'StockDB', 'sa', '292929' )

    First_Create = False

    # First_Create = True
    # db.Reset_Table( )
    # db.CreatDB( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        db.date = file[ 7:13 ]

        db.ReadCSV( file )

        db.CompareDB( )

        db.WriteDB( First_Create )

        db.cur_db.commit( )

if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( 'The script took {:06.1f} minute !'.format( time.time( ) - start_tmr ) )