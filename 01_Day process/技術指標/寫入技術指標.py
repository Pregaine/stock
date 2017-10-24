# -*- coding: utf-8 -*-

import pyodbc
import os
from datetime import datetime
import pandas as pd
import re
import numpy as np

class DB_TechAnalysis:

    def __init__( self, server, database, username, password ):

        self.df = pd.DataFrame( )
        self.src_df = pd.DataFrame( )

        self.d = { '分': 'TechAnalysis_H',
                   '日': 'TechAnaly_D',
                   '周': 'TechAnalysis_W',
                   '月': 'TechAnalysis_M'
                }

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
        self.date = ''

    def ResetTable( self, data ):

        d = {
            '分' : 'DROP TABLE IF EXISTS TechAnaly_H',
            '日' : 'DROP TABLE IF EXISTS TechAnaly_D',
            '周' : 'DROP TABLE IF EXISTS TechAnaly_W',
            '月' : 'DROP TABLE IF EXISTS TechAnaly_M'
        }

        # Do some setup
        with self.cur_db.execute( d[ data ] ):
            print( 'Successfully Deleter ' + data )

    def CreatTable( self, data ):

        sql_m_cmd = '''
            CREATE TABLE dbo.TechAnalysis_M
            (
            id int NOT NULL IDENTITY (1, 1),
            stock_id int NOT NULL,
            date_id int NOT NULL,
            open_price decimal(10, 2) NULL,
            high_price decimal(10, 2) NULL,
            low_price decimal(10, 2) NULL,
            close_price decimal(10, 2) NULL,
            volume bigint NULL,
            
            ma3 decimal(10, 2) NULL,
            ma6 decimal(10, 2) NULL,
            ma12 decimal(10, 2) NULL,
            ma24 decimal(10, 2) NULL,
            ma36 decimal(10, 2) NULL,
            ma60 decimal(10, 2) NULL,
            ma120 decimal(10, 2) NULL,
            
            rsi2 decimal(10, 2) NULL,
            rsi5 decimal(10, 2) NULL,
            rsi10 decimal(10, 2) NULL,
            
            k9_3 decimal(10, 2) NULL,
            d9_3 decimal(10, 2) NULL,
            k3_2 decimal(10, 2) NULL,
            d3_3 decimal(10, 2) NULL,
            
            mfi4 decimal(10, 2) NULL,
            mfi6 decimal(10, 2) NULL,
            mfi14 decimal(10, 2) NULL,
            
            macd_dif_6 decimal(10, 2) NULL,
            dem_12 decimal(10, 2) NULL,
            osc_6_12_9 decimal(10, 2) NULL,
            
            macd_dif_12 decimal(10, 2) NULL,
            dem_26 decimal(10, 2) NULL,
            osc6_12_26_9 decimal(10, 2) NULL,
            
            willr9 decimal(10, 2) NULL,
            willr18 decimal(10, 2) NULL,
            willr42 decimal(10, 2) NULL,
            willr14 decimal(10, 2) NULL,
            willr24 decimal(10, 2) NULL,
            willr56 decimal(10, 2) NULL,
            willr72 decimal(10, 2) NULL,
            
            plus_di decimal(10, 2) NULL,
            minus_di decimal(10, 2) NULL,
            dx decimal(10, 2) NULL,
            adx decimal(10, 2) NULL,
            
            upperband decimal(10, 2) NULL,
            middleband decimal(10, 2) NULL,
            dnperband decimal(10, 2) NULL,
            
            bb decimal(10, 2) NULL,
            w20 decimal(10, 2) NULL,
            bias20 decimal(10, 2) NULL,
            bias60 decimal(10, 2) NULL
            
            )  ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_M ADD CONSTRAINT
        PK_TechAnalysis_M PRIMARY KEY CLUSTERED 
        (
        id
        ) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        
        ALTER TABLE dbo.TechAnalysis_M ADD CONSTRAINT
        FK_TechAnalysis_M FOREIGN KEY
        (
        id
        ) REFERENCES dbo.TechAnalysis_M
        (
        id
        ) ON UPDATE  NO ACTION 
         ON DELETE  NO ACTION 
            
        ALTER TABLE dbo.TechAnalysis_M SET (LOCK_ESCALATION = TABLE)

        COMMIT'''

        sql_h_cmd = '''
        CREATE TABLE dbo.TechAnalysis_H
        (
        id int NOT NULL IDENTITY (1, 1),
        stock_id int NOT NULL,
        date smalldatetime NOT NULL,
        open_price decimal(10, 2) NULL,
        high_price decimal(10, 2) NULL,
        low_price decimal(10, 2) NULL,
        close_price decimal(10, 2) NULL,
        volume bigint NULL,
        
        ma25 decimal(10, 2) NULL,
        ma50 decimal(10, 2) NULL,
        ma100 decimal(10, 2) NULL,
        ma300 decimal(10, 2) NULL,
        ma600 decimal(10, 2) NULL,
        ma1200 decimal(10, 2) NULL,
        rsi2 decimal(10, 2) NULL,
        rsi4 decimal(10, 2) NULL,
        rsi5 decimal(10, 2) NULL,
        rsi10 decimal(10, 2) NULL,
        k9_3 decimal(10, 2) NULL,
        d9_3 decimal(10, 2) NULL,
        k3_2 decimal(10, 2) NULL,
        d3_3 decimal(10, 2) NULL,
        mfi5 decimal(10, 2) NULL,
        mfi6 decimal(10, 2) NULL,
        mfi14 decimal(10, 2) NULL,

        macd_dif_6 decimal(10, 2) NULL,
        dem_12 decimal(10, 2) NULL,
        osc_6_12_9 decimal(10, 2) NULL,

        macd_dif_12 decimal(10, 2) NULL,
        dem_26 decimal(10, 2) NULL,
        osc6_12_26_9 decimal(10, 2) NULL,

        willr9 decimal(10, 2) NULL,
        willr18 decimal(10, 2) NULL,
        willr42 decimal(10, 2) NULL,
        willr14 decimal(10, 2) NULL,
        willr24 decimal(10, 2) NULL,
        willr56 decimal(10, 2) NULL,
        willr72 decimal(10, 2) NULL,
        plus_di decimal(10, 2) NULL,
        minus_di decimal(10, 2) NULL,
        dx decimal(10, 2) NULL,
        adx decimal(10, 2) NULL,
        upperband decimal(10, 2) NULL,
        middleband decimal(10, 2) NULL,
        dnperband decimal(10, 2) NULL,
        bb decimal(10, 2) NULL,
        w20 decimal(10, 2) NULL,
        bias20 decimal(10, 2) NULL,
        bias60 decimal(10, 2) NULL
        )  ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_H ADD CONSTRAINT
        PK_Table_2 PRIMARY KEY CLUSTERED 
        (
        id
        ) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_H ADD CONSTRAINT
        FK_Table_2_Table_1 FOREIGN KEY
        (
        id
        ) REFERENCES dbo.TechAnalysis_H
        (
        id
        ) ON UPDATE  NO ACTION 
         ON DELETE  NO ACTION 

        ALTER TABLE dbo.TechAnalysis_H SET (LOCK_ESCALATION = TABLE)

        COMMIT'''

        sql_d_cmd = '''
        CREATE TABLE dbo.TechAnaly_D
	    (
	    stock int NOT NULL,
	    date date NOT NULL,
	    open_price decimal(10, 2) NULL,
	    high_price decimal(10, 2) NULL,
	    low_price decimal(10, 2) NULL,
	    close_price decimal(10, 2) NULL,
	    volume decimal(10, 2) NULL,
	    d_pec decimal(10, 2) NULL,
	    w_pec decimal(10, 2) NULL,
	    m_pec decimal(10, 2) NULL,
	    ma3 decimal(10, 2) NULL,
	    ma5 decimal(10, 2) NULL,
	    ma8 decimal(10, 2) NULL,
	    ma10 decimal(10, 2) NULL,
	    ma20 decimal(10, 2) NULL,
	    ma60 decimal(10, 2) NULL,
	    ma120 decimal(10, 2) NULL,
	    ma240 decimal(10, 2) NULL,
	    ma480 decimal(10, 2) NULL,
	    ma600 decimal(10, 2) NULL,
	    ma840 decimal(10, 2) NULL,
	    ma1200 decimal(10, 2) NULL,
	    rsi2 decimal(10, 2) NULL,
	    rsi4 decimal(10, 2) NULL,
	    rsi5 decimal(10, 2) NULL,
	    rsi10 decimal(10, 2) NULL,
	    k9_3 decimal(10, 2) NULL,
	    d9_3 decimal(10, 2) NULL,
	    k3_2 decimal(10, 2) NULL,
	    d3_3 decimal(10, 2) NULL,
	    mfi4 decimal(10, 2) NULL,
	    mfi6 decimal(10, 2) NULL,
	    mfi14 decimal(10, 2) NULL,
	    macd_dif_6 decimal(10, 2) NULL,
	    dem_12 decimal(10, 2) NULL,
	    osc_6_12_9 decimal(10, 2) NULL,
	    macd_dif_12 decimal(10, 2) NULL,
	    dem_26 decimal(10, 2) NULL,
	    osc6_12_26_9 decimal(10, 2) NULL,
	    willr9 decimal(10, 2) NULL,
	    willr18 decimal(10, 2) NULL,
	    willr42 decimal(10, 2) NULL,
	    willr14 decimal(10, 2) NULL,
	    willr24 decimal(10, 2) NULL,
	    willr56 decimal(10, 2) NULL,
	    willr72 decimal(10, 2) NULL,
	    plus_di decimal(10, 2) NULL,
	    minus_di decimal(10, 2) NULL,
	    dx decimal(10, 2) NULL,
	    adx decimal(10, 2) NULL,
	    upperband decimal(10, 2) NULL,
	    middleband decimal(10, 2) NULL,
	    dnperband decimal(10, 2) NULL,
	    bb decimal(10, 2) NULL,
	    w20 decimal(10, 2) NULL,
	    bias20 decimal(10, 2) NULL,
	    bias60 decimal(10, 2) NULL
	    )  ON [PRIMARY]

        CREATE NONCLUSTERED INDEX IX_TechAnaly_stock ON dbo.TechAnaly_D
	    (
	    stock
	    ) WITH( STATISTICS_NORECOMPUTE = OFF, 
	    IGNORE_DUP_KEY = OFF, 
	    ALLOW_ROW_LOCKS = ON, 
	    ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

        CREATE NONCLUSTERED INDEX IX_TechAnaly_date ON dbo.TechAnaly_D
	    (
	    date
	    ) WITH( STATISTICS_NORECOMPUTE = OFF, 
	    IGNORE_DUP_KEY = OFF, 
	    ALLOW_ROW_LOCKS = ON, 
	    ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

        ALTER TABLE dbo.TechAnaly_D SET (LOCK_ESCALATION = TABLE)

        COMMIT'''

        sql_w_cmd = '''
        CREATE TABLE dbo.TechAnalysis_W
        (
        id int NOT NULL IDENTITY (1, 1),
        stock_id int NOT NULL,
        date_id int NOT NULL,
        open_price decimal(10, 2) NULL,
        high_price decimal(10, 2) NULL,
        low_price decimal(10, 2) NULL,
        close_price decimal(10, 2) NULL,
        volume bigint NULL,
        
        ma4 decimal(10, 2) NULL,
        ma12 decimal(10, 2) NULL,
        ma24 decimal(10, 2) NULL,
        ma48 decimal(10, 2) NULL,
        ma96 decimal(10, 2) NULL,
        ma144 decimal(10, 2) NULL,
        ma240 decimal(10, 2) NULL,
        ma480 decimal(10, 2) NULL,
        
        rsi2 decimal(10, 2) NULL,
        rsi3 decimal(10, 2) NULL,
        rsi4 decimal(10, 2) NULL,
        rsi5 decimal(10, 2) NULL,
        rsi10 decimal(10, 2) NULL,
        
        k9_3 decimal(10, 2) NULL,
        d9_3 decimal(10, 2) NULL,
        k3_2 decimal(10, 2) NULL,
        d3_3 decimal(10, 2) NULL,
        
        mfi4 decimal(10, 2) NULL,
        mfi6 decimal(10, 2) NULL,
        mfi14 decimal(10, 2) NULL,

        macd_dif_6 decimal(10, 2) NULL,
        dem_12 decimal(10, 2) NULL,
        osc_6_12_9 decimal(10, 2) NULL,

        macd_dif_12 decimal(10, 2) NULL,
        dem_26 decimal(10, 2) NULL,
        osc6_12_26_9 decimal(10, 2) NULL,

        willr9 decimal(10, 2) NULL,
        willr18 decimal(10, 2) NULL,
        willr42 decimal(10, 2) NULL,
        willr14 decimal(10, 2) NULL,
        willr24 decimal(10, 2) NULL,
        willr56 decimal(10, 2) NULL,
        willr72 decimal(10, 2) NULL,
        
        plus_di decimal(10, 2) NULL,
        minus_di decimal(10, 2) NULL,
        dx decimal(10, 2) NULL,
        adx decimal(10, 2) NULL,
        upperband decimal(10, 2) NULL,
        middleband decimal(10, 2) NULL,
        dnperband decimal(10, 2) NULL,
        bb decimal(10, 2) NULL,
        w20 decimal(10, 2) NULL,
        bias20 decimal(10, 2) NULL,
        bias60 decimal(10, 2) NULL
        )  ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_W ADD CONSTRAINT
        PK_TechAnalysis_W PRIMARY KEY CLUSTERED 
        (
        id
        ) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_W ADD CONSTRAINT
        FK_TechAnalysis_W FOREIGN KEY
        (
        id
        ) REFERENCES dbo.TechAnalysis_W
        (
        id
        ) ON UPDATE  NO ACTION 
         ON DELETE  NO ACTION 

        ALTER TABLE dbo.TechAnalysis_W SET (LOCK_ESCALATION = TABLE)

        COMMIT'''

        table_d = { '月': sql_m_cmd, '日': sql_d_cmd, '周': sql_w_cmd, '分': sql_h_cmd }

        with self.cur_db.execute( table_d[ data ] ):
            print( 'Successfully Create 技術指標 ' + data )

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
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', val ).fetchone( )[ 0 ]
                datelst.append( value.strftime( '%Y%m%d' ) )

        return datelst

    def GetStock( self, stock_id ):

        ft = self.cur_db.execute( 'SELECT TOP 1 symbol FROM Stocks Where id = ?', (stock_id,) ).fetchone( )

        return ft[ 0 ]

    def GetDate( self, date_id ):

        ft = self.cur_db.execute( 'SELECT TOP 1 date FROM Dates WHERE id = ?', (date_id,) ).fetchone( )

        # if ft is None:
        #     self.cur_db.execute( 'INSERT INTO Dates ( date ) VALUES ( ? )', (val,) )
        #     return self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', (val,) ).fetchone( )[ 0 ]
        # else:
        #     return ft[ 0 ]

        return ft[ 0 ]

    def GetStockID( self, stock_symbol ):

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ?', (stock_symbol,) ).fetchone( )

        return ft[ 0 ]

    def GetDateID( self, val ):

        print( '查詢日期ID', val )

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', (val,) ).fetchone( )

        if ft is None:
            self.cur_db.execute( 'INSERT INTO Dates ( date ) VALUES ( ? )', (val,) )
            return self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', (val,) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def CompareDB( self, data ):

        # print( table_name, stock_num )

        cmd = 'select date, volume from {0} where stock = {1}'.format( self.d[ data ], self.stock )

        ft = self.cur_db.execute( cmd ).fetchall( )
        lst = [ ]

        for val in ft:

            if data != '分':
                date = val[ 0 ].strftime( '%y%m%d' )
            else:
                date = val[ 0 ].strftime( '%y%m%d%H' )

            volume = val[ 1 ]

            lst.append( ( date, volume ) )

        df = pd.DataFrame( lst, columns = [ '日期', '成交量_資料庫取出' ] )

        # print( df.head( 5 ) )

        left = pd.merge( self.df, df, on = [ '日期' ], how = 'left' )

        left = left[ left[ '成交量_資料庫取出' ] != left[ '成交量' ]  ]

        del left[ '成交量_資料庫取出' ]

        self.df = left

        # self.df = self.df[ ~self.df[ '日期' ].isin( lst ) ]
        # print( data, '刪除重覆寫入', self.df )

    def GetStockDF( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM Tdcc WHERE stock_id = (?)', (stock_id,) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', val ).fetchone( )[ 0 ]
                datelst.append( value.strftime( '%Y%m%d' ) )

        return datelst

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )

        # print( self.df )

    def WriteDB( self, data ):

        table_name = self.d[ data ]

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( self.stock, 'exist DB' )
            return

        for val in lst:

            val.pop( 0 )

            if data != '分':
                cmd = '''SELECT * from {} where stock = '{}' and date = '{}' '''.format( self.d[ data ], self.stock, val[0] )
            else:
                cmd = 'select * from ' + table_name  + ' where stock_id = ? and date = ? '
                dt = datetime.strptime( val[ 0 ], '%y%m%d%H' )
                val[ 0 ] = dt.strftime( "%y-%m-%d %H:%M:%S" )

            # 尋找重覆資料
            ft = self.cur_db.execute( cmd ).fetchone( )

            if ft is not None:
                if data != '分':
                    cmd = 'DELETE FROM {} where stock = {} and date = {}'.format( self.d[ data ], self.stock, val[ 0 ] )
                else:
                    cmd = 'DELETE FROM ' + table_name + ' where stock_id = ? and date = ?'

                with self.cur_db.execute( cmd ):
                    print( '刪除重覆資料', self.stock, val[ 0 ] )

            var_string = ', '.join( '?' * ( len( val ) + 1 ) )

            val.insert( 0, self.stock )

            query_string = 'INSERT INTO {} VALUES ( {} )'.format( self.d[ data ], var_string )

            with self.cur_db.execute( query_string, val ):
                print( '寫入', self.stock, val[ 0 ] )

def main( ):

    server = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    db_M = DB_TechAnalysis( server, database, username, password )
    db_W = DB_TechAnalysis( server, database, username, password )
    db_D = DB_TechAnalysis( server, database, username, password )
    db_H = DB_TechAnalysis( server, database, username, password )

    # 移除表格
    # db_M.Reset_Table( '月' )
    # db_W.Reset_Table( '周' )
    # db_D.ResetTable( '日' )
    # db_H.Reset_Table( '分' )

    # 建立資料表
    # db_M.CreatDB( '月' )
    # db_W.CreatDB( '周' )
    # db_D.CreatTable( '日' )
    # db_H.CreatDB( '分' )

    stock_d = {
            '分': [ db_H, '_60分線技術指標.csv'],
            '日': [ db_D, '_日線技術指標.csv'],
            '周': [ db_W, '_周線技術指標.csv'],
            '月': [ db_M, '_月線技術指標.csv'] }

    start_tmr = datetime.now( )

    # 讀取資料夾
    for file in os.listdir( '.\\' ):

        if file.endswith( ".csv" ) != 1:
            continue

        # print( file )

        # 讀取股號
        num = re.match( '\d*', file ).group( 0 )
        data = file[ -10:-9 ]

        # if data in stock_d.keys( ):
        if data == '日':

            Obj = stock_d[ data ][ 0 ]
            path = num + stock_d[ data ][ 1 ]

            # 讀取來源檔
            Obj.stock = num

            print( '讀取', path )
            Obj.ReadCSV( path )

            # 刪去重覆資料
            Obj.CompareDB( data )

            # 寫入資料庫
            Obj.WriteDB( data )

            Obj.cur_db.commit( )
        else:
            print( '讀取錯誤', data )

    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':

    main( )