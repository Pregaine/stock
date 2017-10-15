# -*- coding: utf-8 -*-

import pyodbc
import os
from datetime import datetime
import pandas as pd


class DB_Revenue:
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
        with self.cur_db.execute( '''DROP TABLE IF EXISTS Revenue;''' ):
            print( 'Successfuly Deleter all Table' )

    def CreatDB( self ):

        with self.cur_db.execute( '''

            CREATE TABLE dbo.Revenue
        	(
                id int NOT NULL IDENTITY (1, 1),
                stock_id int NOT NULL,
                date_id int NOT NULL,
                
                Month_Revenue int,
                Last_Month_Revenue int,
                Last_Year_Revenue int,
                
                Last_Month_Ratio float,
                Last_Year_Ration float,
                
                Month_Acc_Revenue bigint,
                Last_Year_Acc_Revenue bigint,
                
                ration float

        	)  ON [PRIMARY]

            ALTER TABLE dbo.Revenue ADD CONSTRAINT
        	PK_Revenue PRIMARY KEY CLUSTERED 
        	(
        	    id
        	) WITH( STATISTICS_NORECOMPUTE = OFF, 
        	IGNORE_DUP_KEY = OFF, 
        	ALLOW_ROW_LOCKS = ON, 
        	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

            ALTER TABLE dbo.Revenue SET ( LOCK_ESCALATION = TABLE )

            COMMIT''' ):
            print( 'Successfuly Create 營收' )

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

    def GetDateID( self, year, month ):

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE YEAR(Date) = ? AND MONTH(Date) = ?',
                                  (year, month) ).fetchone( )

        val = year + month + '15'

        if ft is None:
            self.cur_db.execute( 'INSERT INTO Dates ( date ) VALUES ( ? )', (val,) )
            return self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', (val,) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def CompareDB( self, date_id ):

        ft = self.cur_db.execute( 'select * from Revenue where date_id = ?', (date_id) ).fetchall( )

        data = [ ]

        for val in ft:

            lst = [ 'None' if v is None else v for v in val ]

            lst[ 1 ] = int( self.GetStock( lst[ 1 ] ) )
            lst[ 2 ] = self.GetDate( lst[ 2 ] ).strftime( '%y%m%d' )

            data.append( lst )

        # print( data )

        column = [ 'id', '公司代號', '日期',
                   '當月營收', '上月營收', '去年當月營收',
                   '上月比較增減(%)', '去年同月增減(%)',
                   '當月累計營收', '去年累計營收', '前期比較增減(%)' ]

        self.src_df = pd.DataFrame( data, columns = column )

        del self.src_df[ 'id' ]
        del self.src_df[ '日期' ]

        self.df = pd.concat( [ self.src_df, self.df ], ignore_index = True )
        self.df = self.df.drop_duplicates( [ '公司代號' ], keep = False )

        lst = [ '公司代號', '當月營收', '上月營收', '去年當月營收',
                '上月比較增減(%)', '去年同月增減(%)',
                '當月累計營收', '去年累計營收', '前期比較增減(%)' ]

        self.df = self.df[ lst ]

        # print( self.src_df.iloc[ 0 ] )
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

        del self.df[ '產業別' ]
        del self.df[ '公司名稱' ]

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )
        # print( self.df )

    def WriteDB( self, date_id ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        print( '寫入筆數', len( lst ) )

        for val in lst:

            varlist = [ None if i == u'不適用' else i for i in val ]

            # print( varlist, type( varlist[ 2 ] ) )

            var_string = ', '.join( '?' * (len( varlist ) + 1) )

            query_string = 'INSERT INTO Revenue VALUES ( {} );'.format( var_string )

            try:
                stock_num = self.GetStockID( varlist[ 0 ] )
            except:
                print( varlist[ 0 ], '股號無存在資料庫' )
                continue

            varlist[ 0 ] = date_id

            varlist.insert( 0, stock_num )

            # print( varlist )

            self.cur_db.execute( query_string, (varlist) )

            # print( varlist )


def main( ):
    server = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    db = DB_Revenue( server, database, username, password )

    # db.Reset_Table( )
    # db.CreatDB( )

    start_tmr = datetime.now( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        year = file[ 5:9 ]
        month = file[ 9:11 ]

        db.ReadCSV( file )

        date_id = db.GetDateID( year, month )

        print( year, month, date_id )

        db.CompareDB( date_id )

        db.WriteDB( date_id )
        db.cur_db.commit( )

    print( datetime.now( ) - start_tmr )


if __name__ == '__main__':

    main( )
