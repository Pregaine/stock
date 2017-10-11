# noinspection PyInterpreter
import pyodbc
import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS
import csv
import os
import time
import pandas as pd

class dbHandle( ):

    def __init__( self, server, database, username, password ):

        self.datelst = [ ]
        print( "Initial Database connection..." + database )
        self.dbname = database
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )

    def GetStockList( self ):

        cmd = '''SELECT [symbol] FROM [StockDB].[dbo].[Stocks]'''

        ft = self.cur_db.execute( cmd ).fetchall( )

        return [ val[ 0 ] for val in ft ]

    def Reset_Table( self ):
        # Do some setup
        with self.cur_db.execute( '''
        DROP TABLE IF EXISTS Tdcc;
        ''' ):
            print( 'Successfuly Del all Table' )

    def Create_TDCC(self):

        with self.cur_db.execute( '''
            CREATE TABLE dbo.Tdcc
        	(
                id int NOT NULL IDENTITY (1, 1),
                stock_id int NOT NULL,
                date_id int NOT NULL,
                
                Share_Rating_People_1_999 BIGINT,
                Share_Rating_Unit_1_999 BIGINT,
                Share_Rating_Proportion_1_999 float,
                
                Share_Rating_People_1000_5000 BIGINT,
                Share_Rating_Unit_1000_5000 BIGINT,
                Share_Rating_Proportion_1000_5000 float,
                
                Share_Rating_People_5001_10000 BIGINT,
                Share_Rating_Unit_5001_10000 BIGINT,
                Share_Rating_Proportion_5001_10000 float,
                
                Share_Rating_People_10001_15000 BIGINT,
                Share_Rating_Unit_10001_15000 BIGINT,
                Share_Rating_Proportion_10001_15000 float,
                
                Share_Rating_People_15001_20000 BIGINT,
                Share_Rating_Unit_15001_20000 BIGINT,
                Share_Rating_Proportion_15001_20000 float,
                
                Share_Rating_People_20001_30000 BIGINT,
                Share_Rating_Unit_20001_30000 BIGINT,
                Share_Rating_Proportion_20001_30000 float,
                
                Share_Rating_People_30001_40000 BIGINT,
                Share_Rating_Unit_30001_40000 BIGINT,
                Share_Rating_Proportion_30001_40000 float,
                
                Share_Rating_People_40001_50000 BIGINT,
                Share_Rating_Unit_40001_50000 BIGINT,
                Share_Rating_Proportion_40001_50000 float,
                
                Share_Rating_People_50001_100000 BIGINT,
                Share_Rating_Unit_50001_100000 BIGINT,
                Share_Rating_Proportion_50001_100000 float,
                
                Share_Rating_People_100001_200000 BIGINT,
                Share_Rating_Unit_100001_200000 BIGINT,
                Share_Rating_Proportion_100001_200000 float,
                
                Share_Rating_People_200001_400000 BIGINT,
                Share_Rating_Unit_200001_400000 BIGINT,
                Share_Rating_Proportion_200001_400000 float,
                
                Share_Rating_People_400001_600000 BIGINT,
                Share_Rating_Unit_400001_600000 BIGINT,
                Share_Rating_Proportion_400001_600000 float,
                
                Share_Rating_People_600001_800000 BIGINT,
                Share_Rating_Unit_600001_800000 BIGINT,
                Share_Rating_Proportion_600001_800000 float,
                
                Share_Rating_People_800001_1000000 BIGINT,
                Share_Rating_Unit_800001_1000000 BIGINT,
                Share_Rating_Proportion_800001_1000000 float,
                
                Share_Rating_People_Up_1000001 BIGINT,
                Share_Rating_Unit_Up_1000001 BIGINT,
                Share_Rating_Proportion_Up_1000001 float        
                             
        	)  ON [PRIMARY]

            ALTER TABLE dbo.Tdcc ADD CONSTRAINT
        	PK_Tdcc PRIMARY KEY CLUSTERED 
        	(
        	    id
        	) WITH( STATISTICS_NORECOMPUTE = OFF, 
        	IGNORE_DUP_KEY = OFF, 
        	ALLOW_ROW_LOCKS = ON, 
        	ALLOW_PAGE_LOCKS = ON ) ON [PRIMARY]

            ALTER TABLE dbo.Tdcc SET (LOCK_ESCALATION = TABLE)

            COMMIT
            '''
                          ):
            print( 'Successfuly Create Tdcc' )

    def GetStockID( self, stock_symbol ):

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Stocks WHERE symbol = ?', (stock_symbol,) ).fetchone( )

        return ft[ 0 ]

    def GetDateID(self, val ):

        ft = self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', ( val, ) ).fetchone( )

        if ft is None:
            self.cur_db.execute( 'INSERT INTO Dates ( date ) VALUES ( ? )', ( val,) )
            return self.cur_db.execute( 'SELECT TOP 1 id FROM Dates WHERE date = ?', ( val, ) ).fetchone( )[ 0 ]
        else:
            return ft[ 0 ]

    def GetDateLst( self, value ):

        datelst = [ ]

        stock_id = self.GetStockID( value )

        ft = self.cur_db.execute( 'SELECT date_id FROM Tdcc WHERE stock_id = (?)', ( stock_id, ) ).fetchall( )

        if ft is not None:
            for val in ft:
                value = self.cur_db.execute( 'SELECT date FROM Dates WHERE id = ( ? )', ( val ) ).fetchone( )[ 0 ]
                datelst.append( value.strftime('%Y%m%d') )

        return datelst

    def WriteDB( self, stock, date, df ):

        val = df.values.tolist( )
        varlist = val[ 0 ][ 2: ]
        var_string = ', '.join( '?' * len( val[ 0 ] ) )

        query_string = 'INSERT INTO Tdcc VALUES ( {} );'.format( var_string )

        varlist.insert( 0, date )
        varlist.insert( 0, stock )

        # print( varlist )

        self.cur_db.execute( query_string, ( varlist ) )
        self.cur_db.commit( )


class TdccHandle( ):

    def __init__(self):

        res = requests.get( 'http://www.tdcc.com.tw/smWeb/QryStock.jsp' )
        self.soup = BS( res.text, "html5lib" )
        self.datelst = [ ]
        self.d = { '1': '1_999',
                   '2': '1000_5000',
                   '3': '5001_10000',
                   '4': '10001_15000',
                   '5': '15001_20000',
                   '6': '20001_30000',
                   '7': '30001_40000',
                   '8': '40001_50000',
                   '9': '50001_100000',
                   '10': '100001_200000',
                   '11': '200001_400000',
                   '12': '400001_600000',
                   '13': '600001_800000',
                   '14': '800001_1000000',
                   '15': 'Up_1000001' }

    def qrerry_date(self):

        for date in self.soup.find_all( 'option' ):
            date = '{}'.format( date.text )
            self.datelst.append( date )

        return self.datelst

    def querry_stock( self, date, stock ):

        df = pd.DataFrame( )

        website = 'SCA_DATE=' + date + '&SqlMethod=StockNo&StockNo=' + stock + '&StockName=&sub=%ACd%B8%DF'
        res = requests.get( 'http://www.tdcc.com.tw/smWeb/QryStock.jsp?' + website )
        soup = BS( res.text, "html5lib" )

        tb = soup.select( '.mt' )[ 1 ]
        # name = soup.findAll( 'td', class_ = 'bw09' )[ 0 ]
        # name = ( name.text[ -2: ] )

        df[ 'stock' ] = [ stock ]
        df[ 'date'] = [ date ]

        for tr in tb.select( 'tr' ):

            if tr.select( 'td' )[ 0 ].text in self.d.keys():

                val = tr.select( 'td' )[ 0 ].text
                people = int( tr.select( 'td' )[ 2 ].text.replace( ',', '' ) )
                unit = int( tr.select( 'td' )[ 3 ].text.replace( ',', '' ) )
                proportion = float( tr.select( 'td' )[ 4 ].text.replace( ',', '' ) )

                df[ 'Share_Rating_People_' + self.d[ val ] ] = [ people ]
                df[ 'Share_Rating_Unit_' + self.d[ val ] ] = [ unit ]
                df[ 'Share_Rating_Proportion_' + self.d[ val ] ] = [ proportion ]

        return df

def main( ):
    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = '292929'

    db = dbHandle( server, database, username, password )
    tdcc = TdccHandle( )

    # 刪除集保庫存資料表
    # db.Reset_Table( )

    # 建立集保庫存資料表
    # db.Create_TDCC( )

    # 時間記錄
    start_tmr = datetime.now( )

    # 查詢股號
    stock_lst = db.GetStockList( )[ 0: ]

    # 查詢網路集保庫存日期
    date_lst = tdcc.qrerry_date( )
    print( date_lst )

    # 查詢集保庫存資料表股號擁有日期
    # print( stock_lst[ 0 ], type( stock_lst[ 0 ] ) )
    while len( stock_lst ) != 0:

        stock = stock_lst.pop( 0 )
        # stock = '1101'
        stock_db_date_lst = db.GetDateLst( stock )

        print( stock, '資料庫日期筆數', len( stock_db_date_lst ) )

        # 刪除已抓捉日期
        stock_db_date_lst = list( set( date_lst ) - set( stock_db_date_lst ) )

        while len( stock_db_date_lst ) != 0:

            date = stock_db_date_lst.pop( 0 )

            try:
                # 捉取資料根據日期
                df = tdcc.querry_stock( date, stock )

                # get stock id
                stock_id = db.GetStockID( stock )
                date_id = db.GetDateID( date )

                # dataframe 寫入資料庫
                db.WriteDB( stock_id, date_id, df )
                print( '寫入日期', stock_id, date_id, date, stock )

            except IndexError:
                print( stock, '查詢無', date, '資料'  )

    # 結束
    print( "end" )
    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':
    main( )