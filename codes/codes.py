# -*- coding: utf-8 -*-
#
# Usage: Load all Taiwan stock code info from csv file
#
# TWSE equities = 上市證券
# TPEx equities = 上櫃證券
#
import csv
import os
from collections import namedtuple
from datetime import datetime, timedelta
from . import fetch as TWSE

ROW = namedtuple( 'StockCodeInfo', [ 'type', 'code', 'name', 'ISIN', 'start', 'market', 'group', 'CFI' ] )
PACKAGE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
TPEX_EQUITIES_CSV_PATH = os.path.join( PACKAGE_DIRECTORY, 'tpex_equities.csv' )
TWSE_EQUITIES_CSV_PATH = os.path.join( PACKAGE_DIRECTORY, 'twse_equities.csv' )

codes = {}
tpex = {}
twse = {}

def read_csv( path, types ):

    global codes, twse, tpex

    one_days_ago = datetime.now( ) - timedelta( hours = 24 )

    try:
        filetime = datetime.fromtimestamp( os.path.getmtime( TWSE_EQUITIES_CSV_PATH ) )
        if filetime < one_days_ago:
            TWSE.GetFile( TWSE_EQUITIES_CSV_PATH )

    except Exception as e:
        TWSE.GetFile( TWSE_EQUITIES_CSV_PATH )
        # print( '{}'.format( e ) )

    with open( path, newline='', encoding='utf_8' ) as csvfile:
        reader = csv.reader( csvfile )
        csvfile.readline( )
        for row in reader:
            row = ROW( *row )
            if row.type == '股票': # or row.type == 'ETF':
                codes[ row.code ] = row

            if types == 'tpex':
                tpex[row.code] = row
            else:
                twse[row.code] = row

# read_csv( TPEX_EQUITIES_CSV_PATH, 'tpex' )
read_csv( TWSE_EQUITIES_CSV_PATH, 'twse' )

# print( codes[ '1101' ] )
# print( codes[ '0050' ] )
# print( codes.keys(), len( codes.keys() ) )