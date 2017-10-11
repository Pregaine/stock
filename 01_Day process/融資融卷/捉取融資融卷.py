import requests
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as BS

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

class Investors:

    def __init__( self, num, EndDate ):

        self.df = pd.DataFrame( )
        self.num = num
        self.edate = EndDate
        self.bdate = 1
        self.text = ''
        self.d = {  '日期':[],

                    '融資買進':[],
                    '融資賣出':[],
                    '融資現償':[],
                    '融資餘額':[],
                    '融資增減': [ ],
                    '融資限額': [ ],
                    '融資使用率': [ ],

                    '融券賣出':[],
                    '融券買進':[],
                    '融券券償':[],
                    '融券餘額':[],
                    '融券增減': [ ],
                    '融券券資比': [ ],

                    '資券相抵':[],
                     }

        self.path = self.num + '_融資融卷.csv'
        self.df = pd.DataFrame( )

    def GetYearAgo( self, year = 1 ):

        bdate_obj = datetime.strptime( self.edate, '%Y-%m-%d' ) - timedelta( days = 365 * year )

        self.bdate = bdate_obj.strftime( '%Y-%#m-%d' )

    def GetData( self ):

        url = "http://jdata.yuanta.com.tw/z/zc/zcn/zcn.djhtm"

        querystring = { "a": self.num, "c": self.bdate, "d": self.edate }

        headers = {
            'upgrade-insecure-requests': "1",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36",
            'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            'referer': "http://jdata.yuanta.com.tw/z/zc/zcn/zcn_2330.djhtm",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
            'cookie': "ASPSESSIONIDQQQSASTB=GCPJMEJCPAHAOFPAABEDPEDI; _ga=GA1.3.412065940.1502115769; _gid=GA1.3.1224767635.1505026283",
            'cache-control': "no-cache",
            'postman-token': "e944c758-3182-38ba-a4b8-e7446a7a3ac1"
        }

        response = requests.request( "GET", url, headers = headers, params = querystring )

        self.text = response.text
        # print( response.text )

    def ClearData(self):

        soup = BS( self.text, "html.parser" )

        rows = soup.find_all( 'tr' )

        index = 0

        for row in rows:

            lst = row.select( "td" )

            if len( lst ) == 15:

                if index != 0:

                    tmp_str = lst[ 0 ].string[ 0:3 ]
                    date_str = lst[ 0 ].string.replace( tmp_str, str( int( tmp_str ) + 1911 ) )

                    self.d[ '日期' ].append( date_str )

                    try:
                        self.d[ '融資買進' ].append( int( lst[ 1 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融資買進' ].append( None )

                    try:
                        self.d[ '融資賣出' ].append( int( lst[ 2 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融資賣出' ].append( None )

                    try:
                        self.d[ '融資現償' ].append( int( lst[ 3 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融資現償' ].append( None )

                    try:
                        self.d[ '融資餘額' ].append( int( lst[ 4 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融資餘額' ].append( None )

                    try:
                        self.d[ '融資增減' ].append( int( lst[ 5 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融資增減' ].append( None )

                    try:
                        self.d[ '融資限額' ].append( int( lst[ 6 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融資限額' ].append( None )

                    try:
                        self.d[ '融資使用率' ].append( float( lst[ 7 ].string.replace( "%", "" ) ) )
                    except ValueError:
                        self.d[ '融資使用率' ].append( None )

                    try:
                        self.d[ '融券賣出' ].append( int( lst[ 8 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融券賣出' ].append( None )

                    try:
                        self.d[ '融券買進' ].append( int( lst[ 9 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融券買進' ].append( None )

                    try:
                        self.d[ '融券券償' ].append( int( lst[ 10 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融券券償' ].append( None )

                    try:
                        self.d[ '融券餘額' ].append( int( lst[ 11 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融券餘額' ].append( None )

                    try:
                        self.d[ '融券增減' ].append( int( lst[ 12 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '融券增減' ].append( None )

                    try:
                        self.d[ '融券券資比' ].append( float( lst[ 13 ].string.replace( "%", "" ) ) )
                    except ValueError:
                        self.d[ '融券券資比' ].append( None )

                    try:
                        self.d[ '資券相抵' ].append( int( lst[ 14 ].string.replace( ",", "" ) ) )
                    except ValueError:
                        self.d[ '資券相抵' ].append( None )

                index += 1

        self.df = pd.DataFrame.from_dict( self.d )

        self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%Y/%m/%d" )

    def CombineDF( self ):

        try:
            df_read = pd.read_csv( self.path, sep = ',', encoding = 'utf8', false_values = 'NA', dtype={ '日期': str } )

            df_read[ '日期' ] = pd.to_datetime( df_read[ '日期' ], format = "%y%m%d" )

            self.df = pd.concat( [ self.df, df_read ], ignore_index = True )
            self.df.drop_duplicates( [ '日期' ], keep = 'last', inplace = True )
            self.df.sort_values( by = '日期',  ascending=False, inplace = True )
            self.df.reset_index( drop = True, inplace = True )

        except:
            print( self.path, '無暫存檔' )


    def SaveCSV(self):

        lst = [ '日期',
                '融資買進', '融資賣出', '融資現償', '融資餘額', '融資增減', '融資限額', '融資使用率',
                '融券賣出', '融券買進', '融券券償', '融券餘額', '融券增減', '融券券資比', '資券相抵']

        self.df = self.df[ lst ]

        self.df.to_csv( self.path, sep = ',', encoding = 'utf-8', date_format = '%y%m%d' )

def main( ):

    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = '292929'

    db = dbHandle( server, database, username, password )

    stock_lst = db.GetStockList( )

    start_tmr = datetime.now( )

    year = 1
    now_str = datetime.now( ).strftime( '%Y-%#m-%d' )
    # now_str = "2017-9-4"

    # for stock in [ '9958' ]:
    for stock in stock_lst:
        try:
            investors = Investors( stock, now_str )
            investors.GetYearAgo( year = year )
            investors.GetData( )
            investors.ClearData( )
            investors.CombineDF( )
            investors.SaveCSV( )
        except:
            print( stock, 'no data' )

        print( stock, '結束',investors.edate, '開始', investors.bdate )

    print( datetime.now( ) - start_tmr )

if __name__ == '__main__':

        main( )