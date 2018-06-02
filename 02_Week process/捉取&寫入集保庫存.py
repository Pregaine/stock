# coding=utf8

import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS
import module.db.stock_db as DB
import module.inquire.GetStockNum as GetStockNum
import pandas as pd
import codes.codes as TWSE
import sys

class TDCC_Handle:

    def __init__(self):

        # self.soup = BS( res.text, "html5lib" )
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

    def querry_stock( self, stock, date, res ):

        df = pd.DataFrame( )
        soup = BS( res.text, "html5lib" )

        # print( res.text )
        try:
            tb = soup.select( '.mt' )[ 1 ]
        except:
            print( stock, date, soup.findAll( 'script' )[ 2 ].text )
            return

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

    try:
        DB_OBJ = DB.Handle( 'localhost', 'StockDB', 'TDCC',  'sa', 'admin' )
    except Exception as e:
        DB_OBJ = DB.Handle( 'localhost', 'StockDB', 'TDCC', 'sa', '292929' )

    # DB_OBJ.ResetTable( 'TDCC' )
    # DB_OBJ.CreateTable_TDCC( )

    TDCC_OBJ = TDCC_Handle( )

    stock_lst = list( TWSE.codes.keys() )
    # stock_lst = [ '2316' ]

    # 查詢網路集保庫存日期
    # date_lst = TDCC_OBJ.qrerry_date( )
    date_lst = [ "20180525", "20180518", "20180511","20180504",
                 "20180427","20180420","20180413","20180403","20180331",
                 "20180323","20180316","20180309","20180302","20180223",
                 "20180214","20180209","20180202","20180126","20180119",
                 "20180112","20180105","20171229","20171222","20171215",
                 "20171208","20171201","20171124","20171117","20171110",
                 "20171103","20171027","20171020","20171013","20171006",
                 "20170930","20170922","20170915","20170908","20170901",
                 "20170825","20170818","20170811","20170804","20170728",
                 "20170721","20170714","20170707","20170630","20170623",
                 "20170616","20170609","20170603","20170526","20170519",
                 "20170512","20170505","20170428","20170421" ]

    date_lst = sys.argv[ 1: ]

    # print( date_lst )

    url = "http://www.tdcc.com.tw/smWeb/QryStockAjax.do"

    headers = { 'origin' : "http://www.tdcc.com.tw", 'upgrade-insecure-requests': "1",
        'content-type'   : "application/x-www-form-urlencoded",
        'user-agent'     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
        'accept'         : "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'referer'        : "http://www.tdcc.com.tw/smWeb/QryStockAjax.do", 'accept-encoding': "gzip, deflate",
        'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        'cache-control': "no-cache" }

    while len( stock_lst ) != 0:

        stock = stock_lst.pop( 0 )

        stock_db_date_lst = DB_OBJ.GetAllData( 'date', "stock = \'{}\' ".format( stock ) )
        stock_db_date_lst = list( set( date_lst ) - set( stock_db_date_lst ) )

        print( '{} 資料庫日期筆數 {}'.format( stock, len( stock_db_date_lst ) ) )

        while len(  stock_db_date_lst  ) != 0:

            date = stock_db_date_lst.pop( 0 )

            payload = {
                "REQ_OPR": "SELECT",
                'SqlMethod': 'StockNo',
                'StockName': '', 'StockNo': '{}'.format( stock ),
                'clkStockName'   : '',
                'clkStockNo': '{}'.format( stock ),
                'radioStockNo': '{}'.format( stock ),
                'scaDate' : '{}'.format( date ),
                'scaDates': '{}'.format( date ), }

            response = requests.request( "POST", url, data = payload, headers = headers )
            print( 'Web Return Status', response.status_code, stock )

            if response.status_code != 200:
                stock_db_date_lst.append( date )
                continue

            # 捉取資料根據日期

            soup = BS( response.text, "html5lib" )
            try:
                if soup.find( 'td', align = "center", colspan = "5" ).text == '無此資料':
                    print( '{} {} 無此資料'.format( stock, date ) )
                    continue
            except:
                pass

            df = TDCC_OBJ.querry_stock( stock, date, response )
            # print( df.head() )
            if df.empty:
                continue

            data = df.iloc[ 0 ].tolist( )
            data = data[ 0 : 2 ] + [ float( i ) for i in data[ 2: ] ]
            DB_OBJ.WriteData( data )

            # print( df )
            # stock_db_date_lst.append( date )
            # print( '{} {}寫入資料庫失敗'.format( stock, date ) )

if __name__ == '__main__':

    start_tmr = datetime.now( )
    main( )
    print( datetime.now( ) - start_tmr )