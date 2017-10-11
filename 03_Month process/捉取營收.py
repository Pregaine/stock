import urllib.request
from bs4 import BeautifulSoup
import csv
import os
from datetime import datetime, timedelta

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

                self.ym_lst.append( str( year + 1911 ) + '{0:0>2}'.format( month ) )

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

            response = urllib.request.urlopen( url )

            html = response.read( )
            sp = BeautifulSoup( html.decode( 'cp950', 'ignore' ).encode( 'utf-8' ), "html.parser" )
            tblh = sp.find_all( 'table', attrs = { 'border': '0', 'width': '100%' } )

            with open( file, 'w', newline = '', encoding = 'utf-8' )as file:
                w = csv.writer( file )
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

            index += 1



def main( ):

    start_tmr = datetime.now( )

    try:
        RevenueObj = Revenue( )

        RevenueObj.GetUrl_Lst( )

        RevenueObj.GetRevenue( )
    except:
        print( "網頁未存在" )

    print( datetime.now( ) - start_tmr )

# 判斷檔案

# 補齊未捉檔案

# 列印執行時間

# 結束

if __name__ == '__main__':

    main( )