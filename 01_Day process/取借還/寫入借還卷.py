# -*- coding: utf-8 -*-

import pyodbc
import os
from datetime import datetime
import pandas as pd


class DB_Lend:

    def __init__( self, server, database, username, password ):

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

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''DROP TABLE IF EXISTS Lend;''' ):
            print( 'Successfuly Deleter all Table' )

    def CreatDB( self ):

        with self.cur_db.execute( '''

            CREATE TABLE dbo.Lend 
        	(
                id int NOT NULL IDENTITY (1, 1),
                stock_id int NOT NULL,
                date_id int NOT NULL,
                
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

            ALTER TABLE dbo.Lend ADD CONSTRAINT
        	PK_Lend PRIMARY KEY CLUSTERED 
        	(
        	    id
        	) WITH( STATISTICS_NORECOMPUTE = OFF, 
        	IGNORE_DUP_KEY = OFF, 
        	ALLOW_ROW_LOCKS = ON, 
        	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

            ALTER TABLE dbo.Lend SET ( LOCK_ESCALATION = TABLE )

            COMMIT''' ):
            print( 'Successfuly Create 借還券' )

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

    def CompareDB( self, date_id ):

        ft = self.cur_db.execute( 'select * from Lend where date_id = ?', (date_id) ).fetchall( )

        data = [ ]

        for val in ft:

            lst = [ 'None' if v is None else v for v in val ]

            lst[ 1 ] = self.GetStock( lst[ 1 ] )
            # lst[ 2 ] = self.GetDate( lst[ 2 ] ).strftime( '%y%m%d' )

            data.append( lst )

        column = [ 'id', '股票代號', '日期',
                   '借券餘額股', '借券餘額異動股借券',
                   '借券餘額異動股還券', '借券餘額差值',
                   '借券賣出當日餘額', '借券賣出當日賣出',
                   '借券賣出當日還券', '借券賣出當日差值',
                   '借券賣出今日可限額' ]

        self.src_df = pd.DataFrame( data, columns = column )

        del self.src_df[ 'id' ]
        del self.src_df[ '日期' ]

        self.df = pd.concat( [ self.df, self.src_df ], ignore_index = True )
        self.df.drop_duplicates( [ '股票代號' ], keep = False, inplace = True )

        column = [ '股票代號', '借券餘額股', '借券餘額異動股借券',
                   '借券餘額異動股還券', '借券餘額差值',
                   '借券賣出當日餘額', '借券賣出當日賣出',
                   '借券賣出當日還券', '借券賣出當日差值',
                   '借券賣出今日可限額' ]

        self.df = self.df[ column ]

        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( self.df.iloc[ 0 ] )

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

        del self.df[ '證券名稱' ]

        mask = ( self.df[ '股票代號' ].str.len( ) == 4 )

        self.df = self.df.loc[ mask ]

        # print( self.df )

    def WriteDB( self, date_id ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( self.date, 'exist DB' )

        print( '寫入筆數', len( lst ) )

        for val in lst:

            varlist = val

            var_string = ', '.join( '?' * (len( varlist ) + 1) )

            query_string = 'INSERT INTO Lend VALUES ( {} );'.format( var_string )

            try:
                stock_id = self.GetStockID( varlist[ 0 ] )
            except:
                print( varlist[ 0 ], '股號未存在資料庫' )
                continue

            varlist[ 0 ] = date_id

            varlist.insert( 0, stock_id )

            self.cur_db.execute( query_string, (varlist) )


def main( ):

    server = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = '292929'

    db = DB_Lend( server, database, username, password )

    # db.Reset_Table( )
    # db.CreatDB( )

    start_tmr = datetime.now( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        db.date = file[ 7:13 ]

        db.ReadCSV( file )

        date_id = db.GetDateID( db.date )

        db.CompareDB( date_id )

        db.WriteDB( date_id )

        db.cur_db.commit( )

    print( datetime.now( ) - start_tmr )


if __name__ == '__main__':

    main( )