# -*- coding: utf-8 -*-

import pyodbc
import re
import os
import csv
from datetime import datetime
import glob

def resetTable( cur ):
    # Do some setup
    with cur.execute( '''
    DROP TABLE IF EXISTS DailyTrades;
    DROP TABLE IF EXISTS Brokerages;
    DROP TABLE IF EXISTS Stocks;
    DROP TABLE IF EXISTS Dates;
    '''
    ):
        print( 'Successfuly Del all Table' )

    # DROP TABLE IF EXISTS Brokerages;
    # DROP TABLE IF EXISTS Stocks;
    # DROP TABLE IF EXISTS Dates;


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
        print( 'Successfuly Create Brokerages' )

class dbHandle( ):

    con_db = None
    cur_db = None
    dbname = None
    dirs = None

    def __init__( self, server, database, username, password ):

        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )

        self.date = ''

        self.date_id = None

        self.stock_id = None

        self.con_db.commit( )

    def _insertGetID( self, cur, tablename, fieldname, value ):

        ft = cur.execute( 'SELECT id FROM ' + tablename + ' WHERE ' + fieldname + ' = (?)', (value,) ).fetchone( )

        if ft is None:
            cur.execute( 'INSERT INTO ' + tablename + ' (' + fieldname + ') VALUES (?)', (value,) )
            return cur.execute( 'SELECT id FROM ' + tablename + ' WHERE ' + fieldname + ' = (?)', (value,) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def insert_csv2DB( self, filename ):

        f = open( filename, 'r', encoding = 'utf8', errors = 'ignore' )

        for row in csv.reader( f ):
            if row[ 0 ] == '':
                return 0

            brokerage_symbol = row[ 1 ][ 0:4 ]
            brokerage_name = row[ 1 ][ 4:len( row[ 1 ] ) ].replace( ' ', '' ).replace( '\u3000', '' )
            price = row[ 2 ]

            buy_volume = row[ 3 ]
            sell_volume = row[ 4 ]

            # brokerage
            ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Brokerages WHERE symbol = ? collate Chinese_Taiwan_Stroke_CS_AS',
                                      ( brokerage_symbol, ) ).fetchone( )

            # print( brokerage_symbol, brokerage_name )

            if ft is None:
                self.cur_db.execute( 'INSERT INTO Brokerages ( symbol, name ) VALUES ( ?, ? )'
                                     ,brokerage_symbol, brokerage_name )

                self.cur_db.execute( 'SELECT TOP 1 id FROM Brokerages WHERE symbol = ? collate Chinese_Taiwan_Stroke_CS_AS'
                                     , brokerage_symbol )

                brokerage_id = self.cur_db.fetchone( )[ 0 ]
            else:
                brokerage_id = ft[ 0 ]

            data = ( brokerage_id, self.stock_id, self.date_id, price, buy_volume, sell_volume )

            # print( data )
            # DailyTrades
            self.cur_db.execute(
                'INSERT INTO DailyTrades ( brokerage_id, stock_id, date_id, price, buy_volume, sell_volume ) \
                 VALUES ( ?, ?, ?, ?, ?, ? )', data )

    def insertDB( self ):

        for d in glob.glob( './/**//*.csv' ):

            # if d.endswith( ".csv" ) != 1:
                # continue

            self.date = d[ -10:-4 ]

            filenames = d

            out_str = re.sub( '[0-9]+\\\\', '', filenames.split( '_' )[ 1 ] )

            stock_symbol = re.match( '\d*', out_str ).group( 0 )
            stock_name = re.sub( '\d*', '', out_str )
            # ----------------------------------------

            print( stock_symbol, stock_name, self.date )

            # stock
            ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ?', (stock_symbol,) ).fetchone( )

            if ft is None:
                self.cur_db.execute( 'INSERT INTO Stocks (symbol, name) VALUES ( ?, ? )',
                                     ( stock_symbol, stock_name ) )

                self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ?',
                                     (stock_symbol,) )

                self.stock_id = self.cur_db.fetchone( )[ 0 ]
            else:
                self.stock_id = ft[ 0 ]

            # date
            self.date_id = self._insertGetID( self.cur_db, 'Dates', 'date', self.date )

            ft = self.cur_db.execute( 'SELECT stock_id, date_id \
                                       FROM DailyTrades WHERE stock_id = ? and date_id = ?',
                                      ( self.stock_id, self.date_id ) ).fetchone( )

            if ft is None:
                self.insert_csv2DB( filenames )
                self.con_db.commit( )
            else:
                print( '資料已存在', filenames )

def main( ):

    start = datetime.now( )
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = "292929"

    db = dbHandle( server, database, username, password )

    # resetTable( db.cur_db ) # 這句會刪掉SQL內所有資料
    # CreateDailyTrade( db.cur_db )
    # CreateBrokerages( db.cur_db )
    # CreateDates( db.cur_db )
    # CreateStocks( db.cur_db )

    db.insertDB( )
    print( datetime.now( ) - start )

if __name__ == '__main__':
    main( )