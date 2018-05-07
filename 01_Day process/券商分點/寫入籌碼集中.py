# -*- coding: utf-8 -*-
import pyodbc
import re
import csv
from datetime import datetime
import glob
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import shutil
import os

class dbHandle:

    def __init__( self, server, database, username, password ):

        cmd = """SET LANGUAGE us_english; set dateformat ymd;"""
        print( "Initial Database connection..." + database )
        self.con_db = pyodbc.connect( 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +
                                      ';PORT=1443;DATABASE=' + database +
                                      ';UID=' + username +
                                      ';PWD=' + password )

        self.cur_db = self.con_db.cursor( )
        self.date = ''
        self.stock = None
        self.con_db.commit( )
        self.cur_db.execute( cmd )

    def ResetTable( self, table ):

        cmd = 'DROP TABLE IF EXISTS ' + table

        # Do some setup
        with self.cur_db.execute( cmd ):
            print( 'Successfully Del' + table )

    def CreateTable( self ):

        """確認一定長度，且只會有英數字：char
                        確認一定長度，且可能會用非英數以外的字元：nchar
                        長度可變動，且只會有英數字：varchar
                        長度可變動，且可能會用非英數以外的字元：nvarchar
                        price decimal( 8, 2 ) NOT NULL
                        最大位數8位數包含小數點前6位，小數點後2位，或小數點前8位，小數點無"""

        cols = [ '股號', '日期', '收盤',
                 '01天集中%', '03天集中%', '05天集中%', '10天集中%', '20天集中%', '60天集中%',
                 '01天佔股本比重', '03天佔股本比重',
                 '05天佔股本比重', '10天佔股本比重', '20天佔股本比重', '60天佔股本比重',
                 '01天周轉率', '03天周轉率', '05天周轉率', '10天周轉率', '20天周轉率',
                 '60天周轉率',
                 '01天主力買賣超(張)', '03天主力買賣超(張)', '05天主力買賣超(張)', '10天主力買賣超(張)', '20天主力買賣超(張)', '60天主力買賣超(張)',
                 '01天家數差', '03天家數差', '05天家數差', '10天家數差', '20天家數差', '60天家數差', ]

        cmd = ''' CREATE TABLE dbo.CONCENTRATION
        (
        stock varchar(10) COLLATE Chinese_Taiwan_Stroke_CS_AS NOT NULL,
        date date NOT NULL,
        close_price decimal( 6, 1 ) NOT NULL,
        
        one            decimal( 6, 1 ),
        three          decimal( 5, 1 ),
        five            decimal( 5, 1 ),
        ten             decimal( 5, 1 ),
        twenty        decimal( 5, 1 ),
        sixty           decimal( 5, 1 ),
          
	  capital_weight_01 decimal( 6, 2 ) NULL,
	  capital_weight_03 decimal( 6, 2 ) NULL,
	  capital_weight_05 decimal( 6, 2 ) NULL,
	  capital_weight_10 decimal( 6, 2 ) NULL,
	  capital_weight_20 decimal( 6, 2 ) NULL,
	  capital_weight_60 decimal( 6, 2 ) NULL,
	  
	  turnover_01 decimal( 6, 2 ) NULL,
	  turnover_03 decimal( 6, 2 ) NULL,
	  turnover_05 decimal( 6, 2 ) NULL,
	  turnover_10 decimal( 6, 2 ) NULL,
	  turnover_20 decimal( 6, 2 ) NULL,
	  turnover_60 decimal( 6, 2 ) NULL,
	  
	  main_force_01 decimal( 8, 0 ) NULL,
	  main_force_03 decimal( 8, 0 ) NULL,
	  main_force_05 decimal( 8, 0 ) NULL,
	  main_force_10 decimal( 8, 0 ) NULL,
	  main_force_20 decimal( 8, 0 ) NULL,
	  main_force_60 decimal( 8, 0 ) NULL,
	  
	  bscnt_01 decimal( 4, 0 ) NULL,
	  bscnt_03 decimal( 4, 0 ) NULL,
	  bscnt_05 decimal( 4, 0 ) NULL,
	  bscnt_10 decimal( 4, 0 ) NULL,
	  bscnt_20 decimal( 4, 0 ) NULL,
	  bscnt_60 decimal( 4, 0 ) NULL, 

	)  ON [PRIMARY]

        COMMIT'''

        with self.cur_db.execute( cmd ):
            print( 'Successfully Create CONCENTRATION 集中度' )


#  todo driver 修正
def conn():
    return pyodbc.connect(
    'DRIVER={ODBC Driver 13 for SQL Server};' +
    'SERVER=localhost;' + 'PORT=1443;' +
    'DATABASE=StockDB;' +
    'UID=sa;' + 'PWD=admin;' )

# Create the sqlalchemy engine using the pyodbc connection
engine = create_engine( "mssql+pyodbc://?driver=ODBC+Driver+13?charset=utf8", creator = conn )
con = engine.connect( )
con.execute( "SET LANGUAGE us_english; set dateformat ymd;" )
con.close()

def main():

    cols = [ 'stock', 'date', 'close_price',
             'one', 'three', 'five', 'ten', 'twenty', 'sixty',
             'capital_weight_01', 'capital_weight_03', 'capital_weight_05', 'capital_weight_10', 'capital_weight_20', 'capital_weight_60',
             'turnover_01', 'turnover_03', 'turnover_05', 'turnover_10', 'turnover_20', 'turnover_60',
             'main_force_01', 'main_force_03', 'main_force_05', 'main_force_10', 'main_force_20', 'main_force_60',
             'bscnt_01', 'bscnt_03', 'bscnt_05', 'bscnt_10', 'bscnt_20', 'bscnt_60' ]

    path = '籌碼集中暫存.csv'

    try:
        db = dbHandle( 'localhost', 'StockDB', 'sa', "admin" )
    except Exception as e:
        db = dbHandle( 'localhost', 'StockDB', 'sa', "292929" )
        print( '{}'.format( e ) )

    # db.ResetTable( 'CONCENTRATION' )
    # db.CreateTable(  )

    csv_df = pd.read_csv( path, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '股號': str, '日期':str }
    , parse_dates = [ 1 ] )

    del csv_df[ 'Unnamed: 0' ]
    csv_df.columns = cols
    print( csv_df.head( ) )

    cmd = 'SELECT stock, date FROM [dbo].[CONCENTRATION] ORDER BY date DESC '
    db_df = pd.read_sql_query( cmd, engine  )

    # big_df = pd.concat( [ db_df, csv_df ], ignore_index=True )
    # print( db_df.head() )

    # big_df.drop_duplicates( keep = False, inplace = True, subset=['stock', 'date'] )
    # print( big_df )

    csv_df.to_sql( name = 'CONCENTRATION', con = engine, index = False, if_exists = 'replace', index_label = None )

if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( 'The script took {:06.1f} minute !'.format( (time.time( ) - start_tmr) / 60 ) )