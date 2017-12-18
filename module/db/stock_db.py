# -*- coding: utf-8 -*-

import pyodbc

class Handle:

    def __init__( self, server, database, table, username, password ):

        self.date = None

        self.table = table

        self.stock = None

        print( "Initial Database Connection... " + database )

        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )

        self.con_db.commit( )


    def ResetTable( self, table ):

        cmd = 'DROP TABLE IF EXISTS {}'.format( table )

        # Do some setup
        with self.cur_db.execute( cmd ):
            print( 'Successfully Del {}'.format( table ) )

    def CreateTable_BrokerageAvgPrice( self ):

        """建立卷商買賣成本資料表.
                """

        cmd = ''' 
        CREATE TABLE dbo.BrokerageAvgPrice
        (
        stock varchar(10) NOT NULL,
        date date NOT NULL,
        avg_buy_price decimal(6, 2) NULL,
        avg_sell_price decimal(6, 2) NULL,
        rate decimal(3, 2) NULL
        )  ON [PRIMARY]
        COMMIT'''

        with self.cur_db.execute( cmd ):
            print( '成功建立卷商買賣均價表格' )

    def CreateTable_TDCC(self):

        """建立集保庫存資料表"""

        cmd = '''
        CREATE TABLE dbo.TDCC
        (
        stock varchar(10) NOT NULL,
        date date NOT NULL,
        
        Share_Rating_People_1_999 decimal(18, 0),
        Share_Rating_Unit_1_999 decimal(18, 0),
        Share_Rating_Proportion_1_999 decimal(4, 2),
        
        Share_Rating_People_1000_5000 decimal(18,0),
        Share_Rating_Unit_1000_5000 decimal(18,0),
        Share_Rating_Proportion_1000_5000 decimal(4, 2),
        
        Share_Rating_People_5001_10000 decimal(18,0),
        Share_Rating_Unit_5001_10000 decimal(18,0),
        Share_Rating_Proportion_5001_10000 decimal(4, 2),
        
        Share_Rating_People_10001_15000 decimal(18,0),
        Share_Rating_Unit_10001_15000 decimal(18,0),
        Share_Rating_Proportion_10001_15000 decimal(4, 2),
        
        Share_Rating_People_15001_20000 decimal(18,0),
        Share_Rating_Unit_15001_20000 decimal(18,0),
        Share_Rating_Proportion_15001_20000 decimal(4, 2),
        
        Share_Rating_People_20001_30000 decimal(18,0),
        Share_Rating_Unit_20001_30000 decimal(18,0),
        Share_Rating_Proportion_20001_30000 decimal(4, 2),
        
        Share_Rating_People_30001_40000 decimal(18,0),
        Share_Rating_Unit_30001_40000 decimal(18,0),
        Share_Rating_Proportion_30001_40000 decimal(4, 2),
        
        Share_Rating_People_40001_50000 decimal(18,0),
        Share_Rating_Unit_40001_50000 decimal(18,0),
        Share_Rating_Proportion_40001_50000 decimal(4, 2),
        
        Share_Rating_People_50001_100000 decimal(18,0),
        Share_Rating_Unit_50001_100000 decimal(18,0),
        Share_Rating_Proportion_50001_100000 decimal(4, 2),
        
        Share_Rating_People_100001_200000 decimal(18,0),
        Share_Rating_Unit_100001_200000 decimal(18,0),
        Share_Rating_Proportion_100001_200000 decimal(4, 2),
        
        Share_Rating_People_200001_400000 decimal(18,0),
        Share_Rating_Unit_200001_400000 decimal(18,0),
        Share_Rating_Proportion_200001_400000 decimal(4, 2),
        
        Share_Rating_People_400001_600000 decimal(18,0),
        Share_Rating_Unit_400001_600000 decimal(18,0),
        Share_Rating_Proportion_400001_600000 decimal(4, 2),
        
        Share_Rating_People_600001_800000 decimal(18,0),
        Share_Rating_Unit_600001_800000 decimal(18,0),
        Share_Rating_Proportion_600001_800000 decimal(4, 2),
        
        Share_Rating_People_800001_1000000 decimal(18,0),
        Share_Rating_Unit_800001_1000000 decimal(18,0),
        Share_Rating_Proportion_800001_1000000 decimal(4, 2),
        
        Share_Rating_People_Up_1000001 decimal(18,0),
        Share_Rating_Unit_Up_1000001 decimal(18,0),
        Share_Rating_Proportion_Up_1000001 decimal(4, 2)        
                             
        )  ON [PRIMARY]
        
        COMMIT'''

        with self.cur_db.execute( cmd ):
            print( '成功建立集保庫存表格' )

    def GetData( self, field, condition ):

        cmd = 'SELECT {} FROM {} WHERE {}'.format( field, self,table, condition )

        ft = self.cur_db.execute( cmd ).fetchone( )

        return ft

    def GetAllData(self, field, condition ):

        lst = [ ]

        cmd = "SELECT {} FROM {} WHERE {}".format( field, self.table, condition )

        ft = self.cur_db.execute( cmd ).fetchall( )

        if ft is not None:
            for val in ft:
                lst.append( val[ 0 ].strftime( '%Y%m%d' ) )

        return lst

    def WriteData( self, data ):

        var_string = ', '.join( '?' * ( len( data )  ) )

        cmd = 'INSERT INTO {} VALUES ( {} )'.format( self.table, var_string )

        with self.cur_db.execute( cmd, data ):

            self.stock = data[ 0 ]
            self.date = data[ 1 ]
            self.con_db.commit( )

            print( '寫入 {} 資料表 {} {}'.format( self.table, self.stock, self.date ) )

if __name__ == '__main__':

    pass


