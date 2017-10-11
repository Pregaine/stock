# -*- coding: utf-8 -*-

import pyodbc
import re
import os
import csv
from datetime import datetime
import pandas as pd

class DB_MarginTrad:

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

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''DROP TABLE IF EXISTS MarginTrad;''' ):
            print( 'Successfuly Deleter all Table' )

    def CreatDB(self):

        with self.cur_db.execute( '''
        
            CREATE TABLE dbo.MarginTrad
        	(
                id int NOT NULL IDENTITY (1, 1),
                stock_id int NOT NULL,
                date_id int NOT NULL,

                Financing_Buy int,
                Financing_Sell int,
                Financing_PayOff int,
                Financing_Over int,
                Financing_Increase int,
                Financing_Limit int,
                Financing_Use float,
                
                Margin_Sell int,
                Margin_Buy int,
                Margin_PayOff int,
                Margin_Over int,
                Margin_Increase int,
                Margin_Ratio float,
                Margin_Offset int
                              
        	)  ON [PRIMARY]

            ALTER TABLE dbo.MarginTrad ADD CONSTRAINT
        	PK_MarginTrad PRIMARY KEY CLUSTERED 
        	(
        	    id
        	) WITH( STATISTICS_NORECOMPUTE = OFF, 
        	IGNORE_DUP_KEY = OFF, 
        	ALLOW_ROW_LOCKS = ON, 
        	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]
        	
            ALTER TABLE dbo.MarginTrad SET ( LOCK_ESCALATION = TABLE )

            COMMIT''' ):
            print( 'Successfuly Create 融資融券' )

    def GetStockList( self ):

        cmd = '''SELECT [symbol] FROM [StockDB].[dbo].[Stocks]'''

        ft = self.cur_db.execute( cmd ).fetchall( )

        return [ val[ 0 ] for val in ft ]

    def GetDateLst( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM MarginTrad WHERE stock_id = (?)', ( stock_id, ) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', ( val ) ).fetchone( )[ 0 ]
                datelst.append( value.strftime('%Y%m%d') )

        return datelst

    def GetStock(self, stock_id ):

        ft = self.cur_db.execute( 'SELECT TOP 1 symbol FROM Stocks Where id = ?', ( stock_id, ) ).fetchone( )

        return ft[ 0 ]

    def GetDate(self, date_id ):

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

        ft = self.cur_db.execute( 'select * from MarginTrad where stock_id = ?', ( stock_num ) ).fetchall( )

        data = [ ]

        for val in ft:

            lst = [ 'None' if v is None else v for v in val ]

            lst[ 1 ] = self.GetStock( lst[ 1 ] )
            lst[ 2 ] = self.GetDate( lst[ 2 ] ).strftime( '%y%m%d' )

            data.append( lst )

        column = [ 'id', '股號', '日期',
                    '融資買進', '融資賣出', '融資現償', '融資餘額', '融資增減', '融資限額', '融資使用率',
                    '融券賣出', '融券買進', '融券券償', '融券餘額', '融券增減', '融券券資比', '資券相抵' ]

        self.src_df = pd.DataFrame( data, columns = column )

        del self.src_df[ 'id' ]
        del self.src_df[ '股號' ]

        self.df = pd.concat( [ self.df, self.src_df ], ignore_index = True )
        self.df.drop_duplicates( [ '日期' ], keep = False, inplace = True )
        # self.df.sort_values( by = '日期', ascending = True, inplace = True )
        # self.df.reset_index( drop = True, inplace = True )

        lst = [ '日期',
                   '融資買進', '融資賣出', '融資現償', '融資餘額', '融資增減', '融資限額', '融資使用率',
                   '融券賣出', '融券買進', '融券券償', '融券餘額', '融券增減', '融券券資比', '資券相抵' ]

        self.df = self.df[ lst ]

        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( stock_num, self.df.iloc[ 0 ] )

    def GetStockDF( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM Tdcc WHERE stock_id = (?)', ( stock_id, ) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', ( val ) ).fetchone( )[ 0 ]
                datelst.append( value.strftime('%Y%m%d') )

        return datelst

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )

        # print( self.df )

    def WriteDB( self, stock_num ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( stock_num, 'exist DB' )

        for val in lst:

            varlist = val

            var_string = ', '.join( '?' * ( len( varlist ) + 1 ) )

            query_string = 'INSERT INTO MarginTrad VALUES ( {} );'.format( var_string )

            date_id = self.GetDateID( varlist[ 0 ] )

            varlist[ 0 ] = date_id

            varlist.insert( 0, stock_num )

            self.cur_db.execute( query_string, ( varlist ) )

            print( varlist )

def main( ):

    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = '292929'

    db = DB_MarginTrad( server, database, username, password )

    # db.Reset_Table( )
    # db.CreatDB( )

    start_tmr = datetime.now( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        stock = file[ 0:4 ]

        db.ReadCSV( file )

        stock_id = db.GetStockID( stock )

        db.CompareDB( stock_id )

        db.WriteDB( stock_id )

        db.cur_db.commit( )

    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':

        main( )