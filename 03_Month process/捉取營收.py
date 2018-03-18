import urllib.request
from bs4 import BeautifulSoup
import csv
import os
from datetime import datetime, timedelta
import requests
import pandas as pd

class Revenue:

    def __init__(self):

        self.roc_year = datetime.now( ).strftime( '%Y' )

        self.end_year = int( self.roc_year ) - 1911

        self.path = '上市營收_'

        self.url_lst = [ ]

        self.ym_lst = [ ]

    def GetUrl_Lst( self ):

        for year in range( 99, self.end_year + 1 ):
            for month in range( 1, 13 ):

                pyROCYear = str( year )
                self.url_lst.append( 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_' \
                                     + pyROCYear + '_' + str( month ) + '_0.html' )

                self.ym_lst.append( '{0}{1:0>2}'.format( year + 1911, month ) )

        # print( self.ym_lst )

    def GetRevenue( self ):

        index = 0
        end_file_lst = list( )
        end_file_lst.append( self.path + datetime.now( ).strftime( '%Y%m' ) + '.csv' )

        last_month = datetime.now( ) - timedelta( days = 30 )
        end_file_lst.append( self.path + last_month.strftime( '%Y%m' ) + '.csv' )

        print( '結束日期', end_file_lst )

        for url in self.url_lst:

            print( url )
            file = self.path + self.ym_lst[ index ] + '.csv'

            if file not in end_file_lst:
                if os.path.isfile( file ):
                    print( file, "已存在" )
                    index += 1
                    continue

            headers = {
                'user-agent': "Chrome/61.0.3163.100",
                'upgrade-insecure-requests': "1",
                'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                'accept-encoding': "gzip, deflate",
                'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
                'cookie': "_ga=GA1.3.1953932169.1504347589; _ga=GA1.3.1953932169.1504347589",
                'cache-control': "no-cache",
            }

            data = requests.request( "GET", url, headers = headers )
            data.encoding = 'cp950'

            sp = BeautifulSoup( data.text, "html.parser" )
            tblh = sp.findAll( 'table', attrs = { 'border': '0', 'width': '100%' } )

            if data.status_code == 404:
                print( '網頁回應狀態', data.status_code )
                return

            with open( file, 'w', newline = '', encoding = 'utf-8' )as w_file:
                w = csv.writer( w_file )
                w.writerow( [ u'產業別', u'公司代號', u'公司名稱', u'當月營收', u'上月營收', u'去年當月營收', \
                              u'上月比較增減(%)', u'去年同月增減(%)', u'當月累計營收', u'去年累計營收', u'前期比較增減(%)' ] )

                for h in range( 0, len( tblh ) ):

                    th = tblh[ h ].find( 'th', attrs = { 'align': 'left', 'class': 'tt' } )
                    cls = th.get_text( ).split( '：' )  # 產業別
                    tbl = tblh[ h ].find( 'table', attrs = { 'bordercolor': "#FF6600" } )

                    trs = tbl.find_all( 'tr' )

                    for r in range( 2, len( trs ) ):
                        if r < (len( trs ) - 1 ):
                            tds = trs[ r ].find_all( 'td' )
                            td0 = tds[ 0 ].get_text( )
                            td1 = tds[ 1 ].get_text( )
                            td2 = tds[ 2 ].get_text( ).strip( ).replace( ",", "" )  #
                            td3 = tds[ 3 ].get_text( ).strip( ).replace( ",", "" )  #
                            td4 = tds[ 4 ].get_text( ).strip( ).replace( ",", "" )  #
                            td5 = tds[ 5 ].get_text( ).strip( ).replace( ",", "" )  #
                            td6 = tds[ 6 ].get_text( ).strip( ).replace( ",", "" )  #
                            td7 = tds[ 7 ].get_text( ).strip( ).replace( ",", "" )  #
                            td8 = tds[ 8 ].get_text( ).strip( ).replace( ",", "" )  #
                            td9 = tds[ 9 ].get_text( ).strip( ).replace( ",", "" )

                            rvnlst = (cls[ 1 ], td0, td1, td2, td3, td4, td5, td6, td7, td8, td9)
                            w.writerow( rvnlst )

            df = pd.read_csv( file, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )

            df.drop_duplicates( [ '公司名稱' ], keep = 'last', inplace = True )
            df.to_csv( file, sep = ',', encoding = 'utf-8' )
            index += 1

def main( ):

    RevenueObj = Revenue( )

    try:
        RevenueObj.GetUrl_Lst( )
        RevenueObj.GetRevenue( )
    except:
        print( "網頁未存在" )

if __name__ == '__main__':

    start_tmr = datetime.now( )
    main( )
    print( '{} Sec'.format( datetime.now( ) - start_tmr )  )