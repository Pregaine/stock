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
                   '日': 'TechAnalysis_D',
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

    def Reset_Table( self, data ):

        d = {
            '分' : 'DROP TABLE IF EXISTS TechAnalysis_H',
            '日' : 'DROP TABLE IF EXISTS TechAnalysis_D',
            '周' : 'DROP TABLE IF EXISTS TechAnalysis_W',
            '月' : 'DROP TABLE IF EXISTS TechAnalysis_M'
        }

        # Do some setup
        with self.cur_db.execute( d[ data ] ):
            print( 'Successfuly Deleter ' + data )

    def CreatDB( self, data ):

        sql_m_cmd = '''
            CREATE TABLE dbo.TechAnalysis_M
            (
            id int NOT NULL IDENTITY (1, 1),
            stock_id int NOT NULL,
            date_id int NOT NULL,
            open_price float NULL,
            high_price float NULL,
            low_price float NULL,
            close_price float NULL,
            volume bigint NULL,
            
            ma3 float NULL,
            ma6 float NULL,
            ma12 float NULL,
            ma24 float NULL,
            ma36 float NULL,
            ma60 float NULL,
            ma120 float NULL,
            
            rsi2 float NULL,
            rsi5 float NULL,
            rsi10 float NULL,
            
            k9_3 float NULL,
            d9_3 float NULL,
            k3_2 float NULL,
            d3_3 float NULL,
            
            mfi4 float NULL,
            mfi6 float NULL,
            mfi14 float NULL,
            
            macd_dif_6 float NULL,
            dem_12 float NULL,
            osc_6_12_9 float NULL,
            
            macd_dif_12 float NULL,
            dem_26 float NULL,
            osc6_12_26_9 float NULL,
            
            willr9 float NULL,
            willr18 float NULL,
            willr42 float NULL,
            willr14 float NULL,
            willr24 float NULL,
            willr56 float NULL,
            willr72 float NULL,
            
            plus_di float NULL,
            minus_di float NULL,
            dx float NULL,
            adx float NULL,
            
            upperband float NULL,
            middleband float NULL,
            dnperband float NULL,
            
            bb float NULL,
            w20 float NULL,
            bias20 float NULL,
            bias60 float NULL
            
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
        open_price float NULL,
        high_price float NULL,
        low_price float NULL,
        close_price float NULL,
        volume bigint NULL,
        
        ma25 float NULL,
        ma50 float NULL,
        ma100 float NULL,
        ma300 float NULL,
        ma600 float NULL,
        ma1200 float NULL,
        rsi2 float NULL,
        rsi4 float NULL,
        rsi5 float NULL,
        rsi10 float NULL,
        k9_3 float NULL,
        d9_3 float NULL,
        k3_2 float NULL,
        d3_3 float NULL,
        mfi5 float NULL,
        mfi6 float NULL,
        mfi14 float NULL,

        macd_dif_6 float NULL,
        dem_12 float NULL,
        osc_6_12_9 float NULL,

        macd_dif_12 float NULL,
        dem_26 float NULL,
        osc6_12_26_9 float NULL,

        willr9 float NULL,
        willr18 float NULL,
        willr42 float NULL,
        willr14 float NULL,
        willr24 float NULL,
        willr56 float NULL,
        willr72 float NULL,
        plus_di float NULL,
        minus_di float NULL,
        dx float NULL,
        adx float NULL,
        upperband float NULL,
        middleband float NULL,
        dnperband float NULL,
        bb float NULL,
        w20 float NULL,
        bias20 float NULL,
        bias60 float NULL
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
        CREATE TABLE dbo.TechAnalysis_D
        (
        id int NOT NULL IDENTITY (1, 1),
        stock_id int NOT NULL,
        date_id int NOT NULL,
        open_price float NULL,
        high_price float NULL,
        low_price float NULL,
        close_price float NULL,
        volume bigint NULL,
        
        d_pec float NULL,
        w_pec float NULL,
        m_pec float NULL,
        
        ma3 float NULL,
        ma5 float NULL,
        ma8 float NULL,
        ma10 float NULL,
        ma20 float NULL,
        ma60 float NULL,
        ma120 float NULL,
        ma240 float NULL,
        ma480 float NULL,
        ma600 float NULL,
        ma840 float NULL,
        ma1200 float NULL,
        rsi2 float NULL,
        rsi4 float NULL,
        rsi5 float NULL,
        rsi10 float NULL,
        k9_3 float NULL,
        d9_3 float NULL,
        k3_2 float NULL,
        d3_3 float NULL, 
        
        mfi4 float NULL,
        mfi6 float NULL,
        mfi14 float NULL,
        
        macd_dif_6 float NULL,
        dem_12 float NULL,
        osc_6_12_9 float NULL,
        macd_dif_12 float NULL,
        dem_26 float NULL,
        osc6_12_26_9 float NULL,
        
        willr9 float NULL,
        willr18 float NULL,
        willr42 float NULL,
        willr14 float NULL,
        willr24 float NULL,
        willr56 float NULL,
        willr72 float NULL,
        
        plus_di float NULL,
        minus_di float NULL,
        dx float NULL,
        adx float NULL,
        
        upperband float NULL,
        middleband float NULL,
        dnperband float NULL,
        
        bb float NULL,
        w20 float NULL,
        bias20 float NULL,
        bias60 float NULL,
        
        )  ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_D ADD CONSTRAINT
        PK_TechAnalysis_D PRIMARY KEY CLUSTERED 
        (
        id
        ) WITH( STATISTICS_NORECOMPUTE = OFF, 
        IGNORE_DUP_KEY = OFF, 
        ALLOW_ROW_LOCKS = ON, 
        ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

        ALTER TABLE dbo.TechAnalysis_D SET (LOCK_ESCALATION = TABLE)

        COMMIT'''

        sql_w_cmd = '''
        CREATE TABLE dbo.TechAnalysis_W
        (
        id int NOT NULL IDENTITY (1, 1),
        stock_id int NOT NULL,
        date_id int NOT NULL,
        open_price float NULL,
        high_price float NULL,
        low_price float NULL,
        close_price float NULL,
        volume bigint NULL,
        
        ma4 float NULL,
        ma12 float NULL,
        ma24 float NULL,
        ma48 float NULL,
        ma96 float NULL,
        ma144 float NULL,
        ma240 float NULL,
        ma480 float NULL,
        
        rsi2 float NULL,
        rsi3 float NULL,
        rsi4 float NULL,
        rsi5 float NULL,
        rsi10 float NULL,
        
        k9_3 float NULL,
        d9_3 float NULL,
        k3_2 float NULL,
        d3_3 float NULL,
        
        mfi4 float NULL,
        mfi6 float NULL,
        mfi14 float NULL,

        macd_dif_6 float NULL,
        dem_12 float NULL,
        osc_6_12_9 float NULL,

        macd_dif_12 float NULL,
        dem_26 float NULL,
        osc6_12_26_9 float NULL,

        willr9 float NULL,
        willr18 float NULL,
        willr42 float NULL,
        willr14 float NULL,
        willr24 float NULL,
        willr56 float NULL,
        willr72 float NULL,
        
        plus_di float NULL,
        minus_di float NULL,
        dx float NULL,
        adx float NULL,
        upperband float NULL,
        middleband float NULL,
        dnperband float NULL,
        bb float NULL,
        w20 float NULL,
        bias20 float NULL,
        bias60 float NULL
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
            print( 'Successfuly Create 技術指標 ' + data )

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

    def CompareDB( self, data, stock_num ):

        table_name = self.d[ data ]

        # print( table_name, stock_num )

        if data != '分':
            cmd = 'select date_id, volume from ' + table_name + ' where stock_id = ?'
        else:
            cmd = 'select date, volume from ' + table_name + ' where stock_id = ?'

        ft = self.cur_db.execute( cmd, stock_num ).fetchall( )
        lst = [ ]

        for val in ft:

            # print( val )

            if data != '分':
                date = self.GetDate( val[ 0 ] ).strftime( '%y%m%d' )
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
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', (val) ).fetchone( )[ 0 ]
                datelst.append( value.strftime( '%Y%m%d' ) )

        return datelst

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )

        # print( self.df )

    def WriteDB( self, data, stock_id ):

        table_name = self.d[ data ]

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            print( self.stock, 'exist DB' )
            return

        for val in lst:

            varlist = val[ 1 : ]

            # print( data, '寫入', varlist )

            # print( '寫入欄位', len( varlist ) + 1 )

            if data != '分':
                cmd = 'select * from ' + table_name + ' where stock_id = ? and date_id = ?'
                date_id = self.GetDateID( varlist[ 0 ] )
                varlist[ 0 ] = date_id
            else:
                cmd = 'select * from ' + table_name  + ' where stock_id = ? and date = ? '
                dt = datetime.strptime( varlist[ 0 ], '%y%m%d%H' )
                varlist[ 0 ] = dt.strftime( "%y-%m-%d %H:%M:%S" )

            ft = self.cur_db.execute( cmd, ( stock_id, varlist[ 0 ] ) ).fetchone( )

            if ft is not None:
                if data != '分':
                    cmd = 'DELETE FROM ' + table_name + ' where stock_id = ? and date_id = ?'
                else:
                    cmd = 'DELETE FROM ' + table_name + ' where stock_id = ? and date = ?'

                print( '刪除重覆資料', stock_id, varlist[ 0 ] )
                self.cur_db.execute( cmd, ( stock_id, varlist[ 0 ] ) )

            var_string = ', '.join( '?' * ( len( varlist ) + 1 ) )
            varlist.insert( 0, stock_id )
            query_string = 'INSERT INTO ' + self.d[ data ] + ' VALUES ( {} );'.format( var_string )

            print( '寫入', varlist, len( varlist ) )
            self.cur_db.execute( query_string, varlist )

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
    # db_D.Reset_Table( '日' )
    # db_H.Reset_Table( '分' )

    # 建立資料表
    # db_M.CreatDB( '月' )
    # db_W.CreatDB( '周' )
    # db_D.CreatDB( '日' )
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

        # print( data )

        if data in stock_d.keys( ):
        # if data == '分':

            Obj = stock_d[ data ][ 0 ]
            path = num + stock_d[ data ][ 1 ]

            # 讀取來源檔
            Obj.stock = num

            print( '讀取', path )
            Obj.ReadCSV( path )

            # 讀取股號id
            stock_id = Obj.GetStockID( Obj.stock )

            # 刪去重覆資料
            Obj.CompareDB( data, stock_id )

            # 寫入資料庫
            Obj.WriteDB( data, stock_id )

            Obj.cur_db.commit( )
        else:
            print( '讀取錯誤', data )

    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':

    main( )