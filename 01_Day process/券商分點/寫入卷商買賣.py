# -*- coding: utf-8 -*-

import pyodbc
import re
import csv
from datetime import datetime
import glob

def CreateDailyTrade( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.DailyTrades
	(
        id int NOT NULL IDENTITY (1, 1),
        brokerage_id int NOT NULL,
        stock_id int NOT NULL,
        date_id int NOT NULL,
        price real NOT NULL,
        buy_volume int NOT NULL,
        sell_volume int NOT NULL
	)  ON [PRIMARY]
    
    ALTER TABLE dbo.DailyTrades ADD CONSTRAINT
	PK_DailyTrades PRIMARY KEY CLUSTERED
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF,
	IGNORE_DUP_KEY = OFF,
	ALLOW_ROW_LOCKS = ON,
	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

    ALTER TABLE dbo.DailyTrades SET (LOCK_ESCALATION = TABLE)

    COMMIT
    '''
    ):
        print( 'Successfuly Create Daily Trade')

def CreateStocks( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.Stocks
	(
        id int NOT NULL IDENTITY (1, 1),
        symbol nvarchar(50) NOT NULL,
        name nvarchar(50) NOT NULL
	)  ON [PRIMARY]
	
	ALTER TABLE dbo.Stocks ADD CONSTRAINT
	PK_Stocks PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Stocks ON dbo.Stocks
	(
	name
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Stocks_1 ON dbo.Stocks
	(
	symbol
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Stocks SET (LOCK_ESCALATION = TABLE)

    COMMIT
    ''' ):
        print( 'Successfuly Create Stocks' )

def CreateDates( cur ):

    with cur.execute( '''
    
    CREATE TABLE dbo.Dates
	(
        id int NOT NULL IDENTITY (1, 1),
        date date NOT NULL
	)  ON [PRIMARY]

    ALTER TABLE dbo.Dates ADD CONSTRAINT
	PK_Dates PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dates ON dbo.Dates
	(
	    date
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Dates SET (LOCK_ESCALATION = TABLE)

    COMMIT
    '''):
        print( 'Successfuly Create Dates' )

def CreateBrokerages( cur ):

    with cur.execute( '''
    CREATE TABLE dbo.Brokerages
	(
        id int NOT NULL IDENTITY (1, 1),
        symbol nvarchar(50) COLLATE　Chinese_Taiwan_Stroke_CS_AS NOT NULL,
        name nvarchar(50) NOT NULL
	)  ON [PRIMARY]

    ALTER TABLE dbo.Brokerages ADD CONSTRAINT
	PK_Brokerages PRIMARY KEY CLUSTERED 
	(
	    id
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Brokerages ON dbo.Brokerages
	(
	    name
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE UNIQUE NONCLUSTERED INDEX IX_Brokerages_1 ON dbo.Brokerages
	(
        symbol
	) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.Brokerages SET (LOCK_ESCALATION = TABLE )
    
    COMMIT'''):
        print( 'Successfully Create Brokerages' )

class dbHandle:

    def __init__( self, server, database, username, password ):

        print( "Initial Database connection..." + database )
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )

        self.date = ''

        self.stock = None

        self.con_db.commit( )

    def ResetTable( self, table ):

        cmd = 'DROP TABLE IF EXISTS ' + table

        # Do some setup
        with self.cur_db.execute( cmd ):
            print( 'Successfully Del' + table )

    def CreateTable( self ):

        cmd = ''' CREATE TABLE dbo.DailyTrade
	(
	stock int NOT NULL,
	date date NOT NULL,
	brokerage nvarchar(10) COLLATE　Chinese_Taiwan_Stroke_CS_AS NOT NULL ,
	price decimal(10, 2) NOT NULL,
	buy_volume bigint NULL,
	sell_volume bigint NULL
	)  ON [PRIMARY]

    CREATE NONCLUSTERED INDEX IX_Table_stock ON dbo.DailyTrade
	(
	stock
	) WITH( STATISTICS_NORECOMPUTE = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS = ON, 
	ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE NONCLUSTERED INDEX IX_Table_date ON dbo.DailyTrade
	(
	date
	) WITH( STATISTICS_NORECOMPUTE = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS = ON, 
	ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    CREATE NONCLUSTERED INDEX IX_Table_brokerage ON dbo.DailyTrade
	(
	brokerage
	) WITH( STATISTICS_NORECOMPUTE = OFF, 
	IGNORE_DUP_KEY = OFF, 
	ALLOW_ROW_LOCKS = ON, 
	ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    ALTER TABLE dbo.DailyTrade SET (LOCK_ESCALATION = TABLE)

    COMMIT'''

        with self.cur_db.execute( cmd ):
            print( 'Successfully Create DailyTrade' )


    def InsertCSV2DB( self, filename ):

        f = open( filename, 'r', encoding = 'utf8', errors = 'ignore' )

        for row in csv.reader( f ):
            if row[ 0 ] == '':
                return

            brokerage_symbol = row[ 1 ][ 0:4 ]
            # brokerage_name = row[ 1 ][ 4:len( row[ 1 ] ) ].replace( ' ', '' ).replace( '\u3000', '' )
            price = row[ 2 ]

            buy_volume = row[ 3 ]
            sell_volume = row[ 4 ]

            cmd = 'INSERT INTO DailyTrade ( stock, date, brokerage, price, buy_volume, sell_volume ) \
                   VALUES ( ?, ?, ?, ?, ?, ? )'
            try:
                row = ( self.stock, self.date, brokerage_symbol, price, buy_volume, sell_volume )

                self.cur_db.execute( cmd, row )
            except:
                print( '寫入失敗', row )


    def InsertDB( self ):

        for filename in glob.glob( './/**//*.csv' ):

            # if d.endswith( ".csv" ) != 1:
                # continue

            self.date = filename[ -10:-4 ]

            out_str = re.sub( '[0-9]+\\\\', '', filename.split( '_' )[ 1 ] )

            self.stock = re.match( '\d*', out_str ).group( 0 )
            stock_name = re.sub( '\d*', '', out_str )
            # ----------------------------------------

            print( self.stock, stock_name, self.date )

            # stock
            ft = self.cur_db.execute( 'SELECT stock, date FROM DailyTrade WHERE stock = ? \
                                      and date = ?', ( self.stock, self.date ) ).fetchone( )

            if ft is None:

                self.InsertCSV2DB( filename )

                self.con_db.commit( )
            else:
                print( '資料已存在資料庫 ', filename )

def main( ):

    start = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = "admin"

    db = dbHandle( server, database, username, password )

    # db.ResetTable( 'DailyTrade' )

    # db.CreateTable(  )

    db.InsertDB( )

    print( datetime.now( ) - start )

if __name__ == '__main__':
    main( )