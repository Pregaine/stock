# -*- coding: utf-8 -*-

import pyodbc
import os
import csv
from datetime import datetime
import pandas as pd

class DB_Investors :

    def __init__( self, server, database, username, password ):

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
        self.stock = ''

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''DROP TABLE IF EXISTS Investors;''' ):
            print( 'Successfuly Deleter all Table' )

    def CreatDB( self ):

        with self.cur_db.execute( '''

            CREATE TABLE dbo.Investors 
        	(
                id int NOT NULL IDENTITY (1, 1),
                stock_id int NOT NULL,
                date_id int NOT NULL,
                
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

            ALTER TABLE dbo.Investors ADD CONSTRAINT
        	PK_Investors PRIMARY KEY CLUSTERED 
        	(
        	    id
        	) WITH( STATISTICS_NORECOMPUTE = OFF, 
        	IGNORE_DUP_KEY = OFF, 
        	ALLOW_ROW_LOCKS = ON, 
        	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

            ALTER TABLE dbo.Investors SET ( LOCK_ESCALATION = TABLE )

            COMMIT''' ):
            print( 'Successfuly Create 3大法人' )

    def GetStockList( self ):

        cmd = '''SELECT [symbol] FROM [StockDB].[dbo].[Stocks]'''

        ft = self.cur_db.execute( cmd ).fetchall( )

        return [ val[ 0 ] for val in ft ]

    def GetDateLst( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM MarginTrad WHERE stock_id = (?)', (stock_id,) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', (val) ).fetchone( )[ 0 ]
                datelst.append( value.strftime( '%Y%m%d' ) )

        return datelst

    def GetStock( self, stock_id ):

        ft = self.cur_db.execute( 'SELECT TOP 1 symbol FROM Stocks Where id = ?', (stock_id,) ).fetchone( )

        return ft[ 0 ]

    def GetDate( self, date_id ):

        ft = self.cur_db.execute( 'SELECT TOP 1 date FROM Dates WHERE id = ?', (date_id,) ).fetchone( )

        return ft[ 0 ]

    def GetStockID( self, stock_symbol ):

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ?', (stock_symbol,) ).fetchone( )

        return ft[ 0 ]

    def GetDateID( self, val ):

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', (val,) ).fetchone( )

        if ft is None:
            self.cur_db.execute( 'INSERT INTO Dates ( date ) VALUES ( ? )', (val,) )
            return self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', (val,) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def CompareDB( self, stock_num ):

        # print( stock_num )

        ft = self.cur_db.execute( 'select * from Investors where stock_id = ?', ( stock_num ) ).fetchall( )

        data = [ ]

        for val in ft:

            lst = [ 'None' if v is None else v for v in val ]

            lst[ 1 ] = self.GetStock( lst[ 1 ] )
            lst[ 2 ] = self.GetDate( lst[ 2 ] ).strftime( '%y%m%d' )

            data.append( lst )

        column = [ 'id', '股號', '日期', '外資買賣超', '投信買賣超',
                                        '自營商買賣超', '單日合計買賣超',
                                        '外資估計持股', '投信估計持股',
                                        '自營商估計持股', '單日合計估計持股',
                                        '外資持股比重', '三大法人持股比重' ]

        self.src_df = pd.DataFrame( data, columns = column )

        del self.src_df[ 'id' ]
        del self.src_df[ '股號' ]

        self.df = pd.concat( [ self.df, self.src_df ], ignore_index = True )
        self.df.drop_duplicates( [ '日期' ], keep = False, inplace = True )

        column = [ '日期', '外資買賣超', '投信買賣超',
                   '自營商買賣超', '單日合計買賣超',
                   '外資估計持股', '投信估計持股',
                   '自營商估計持股', '單日合計估計持股',
                   '外資持股比重', '三大法人持股比重' ]

        self.df = self.df[ column ]

        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( stock_num, self.df.iloc[ 0 ] )

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

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )

        # print( self.df )

    def WriteDB( self, stock_num ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( self.stock, 'exist DB')



        for val in lst:

            print( val )

            varlist = val

            var_string = ', '.join( '?' * ( len( varlist ) + 1 ) )

            query_string = 'INSERT INTO Investors VALUES ( {} );'.format( var_string )

            date_id = self.GetDateID( varlist[ 0 ] )

            varlist[ 0 ] = date_id

            varlist.insert( 0, stock_num )

            self.cur_db.execute( query_string, ( varlist ) )

def main( ):

    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = '292929'

    db = DB_Investors( server, database, username, password )

    # db.Reset_Table( )
    # db.CreatDB( )

    start_tmr = datetime.now( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        db.stock = file[ 0:4 ]

        db.ReadCSV( file )

        stock_id = db.GetStockID( db.stock )

        db.CompareDB( stock_id )

        db.WriteDB( stock_id )

        db.cur_db.commit( )

    print( datetime.now( ) - start_tmr )


if __name__ == '__main__':

    main( )