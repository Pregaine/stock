# -*- coding: utf-8 -*-

import pyodbc
import os
from datetime import datetime
import pandas as pd


class DB_Revenue:
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

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''DROP TABLE IF EXISTS REVENUE;''' ):
            print( 'Successfuly Deleter REVENUE Table' )

    def CreatDB( self ):

        with self.cur_db.execute( '''

            CREATE TABLE dbo.REVENUE
        	(
                stock varchar( 10 ) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
                date date NOT NULL,
                
                Month_Revenue decimal( 16, 0 ) NULL,
                Last_Month_Revenue decimal( 16, 0 ) NULL,
                Last_Year_Revenue decimal( 16, 0 ) NULL,
                
                Last_Month_Ratio float NULL,
                Last_Year_Ration float NULL,
                
                Month_Acc_Revenue decimal( 16, 0 ) NULL,
                Last_Year_Acc_Revenue decimal( 16, 0 ) NULL,
                
                ration float NULL
        	) ON [PRIMARY]

            COMMIT ''' ):
            print( 'Successfuly Create 營收' )

    def GetDateLst( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM MarginTrad WHERE stock_id = (?)', (stock_id,) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', (val) ).fetchone( )[ 0 ]
                datelst.append( value.strftime( '%Y%m%d' ) )

        return datelst

    def CompareDB( self, year, month ):

        cmd = 'SELECT stock, Month_Revenue FROM REVENUE WHERE MONTH( date ) = \'{0}\' AND YEAR( date ) = \'{1}\''.format( month, year )
        ft = self.cur_db.execute( cmd ).fetchall( )
        lst = [ ]

        for val in ft:
            stock = val[ 0 ]
            Month_Revenue = val[ 1 ]
            lst.append( ( stock, Month_Revenue ) )

        df_db = pd.DataFrame( lst, columns = [ '公司代號', 'Month_Revenue_FromDB' ] )
        left = pd.merge( self.df, df_db, on = [ '公司代號' ], how = 'left' )

        left = left[ left[ 'Month_Revenue_FromDB' ] != left[ '當月營收' ] ]
        del left[ 'Month_Revenue_FromDB' ]
        self.df = left

        # print( self.df )

    def GetStockDF( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM Tdcc WHERE stock_id = (?)', (stock_id,) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', (val) ).fetchone( )[ 0 ]
                datelst.append( value.strftime( '%Y%m%d' ) )

        return datelst

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '公司代號': str } )

        del self.df[ '產業別' ]
        del self.df[ '公司名稱' ]

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )
        # print( self.df )

    def WriteDB( self, year, month ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( '資料庫比對CSV無新資料 {0}-{1}'.format( year, month ) )
            return

        for val in lst:

            val = [ None if i == u'不適用' else i for i in val ]
            val.pop( 0 )
            val.insert( 1, '{0}-{1}-15'.format( year, month ) )
            print( val )

            var_string = ', '.join( '?' * len( val ) )
            query_string = 'INSERT INTO REVENUE VALUES ( {} );'.format( var_string )

            with self.cur_db.execute( query_string, val ):
                print( '寫入資料庫 {} {}'.format( val[ 0 ], val[ 1 ] ) )



def main( ):

    try:
        db = DB_Revenue( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        db = DB_Revenue( 'localhost', 'StockDB', 'sa', '292929' )

    # db.Reset_Table( )
    # db.CreatDB( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        # if file.endswith( ".csv" ) != 1:
        if file.endswith( "上市營收_201001.csv" ) != 1:
            continue

        year  = file[ 5:9 ]
        month = file[ 9:11 ]
        db.ReadCSV( file )
        db.CompareDB( year, month )
        db.WriteDB( year, month )
        db.cur_db.commit( )

if __name__ == '__main__':

    start_tmr = datetime.now( )
    main( )
    print( datetime.now( ) - start_tmr )