import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import time
from stem import Signal
from stem.control import Controller
import requests

class LendOver:

    def __init__(self):
        # 取得限制日期
        self.datelst = [ ]

        self.df = pd.DataFrame( )

        self.date = ''
        self.path = ''
        self.obj = json.loads( 'NaN' )


    def GetData( self, date_str ):

        print( 'LendOver Get Date {} 證交所借券系統與證商/證金營業處所借券餘額合計表'.format( date_str ) )
        url = "http://www.twse.com.tw/exchangeReport/TWT72U"

        querystring = { "response": "json", "date": date_str, "selectType": "SLBNLB", "_": "1520858126544" }
        headers = { 'accept': "application/json, text/javascript, */*; q=0.01", 'x-requested-with': "XMLHttpRequest",
            'user-agent': "Chrome/64.0.3282.186 Safari/537.36",
            'referer': "http://www.twse.com.tw/zh/page/trading/exchange/TWT72U.html",
            'accept-encoding': "gzip, deflate", 'accept-language': "zh-TW",
            'cookie': "_ga=GA1.3.1838061013.1520518370; _gid=GA1.3.1972994029.1520761897; JSESSIONID=627854663D0CFAB13435F0FA8680B206; _ga=GA1.3.1838061013.1520518370; _gid=GA1.3.1972994029.1520761897; JSESSIONID=FFC6E200FF9907F89C5EEBCDD9DBA2CB; _gat=1",
            'cache-control': "no-cache" }

        response = ''

        while response == '':
            try:
                response = requests.request( "GET", url, headers = headers, params = querystring )
            except requests.exceptions.ConnectionError as e:
                set_new_ip()
                session = get_tor_session( )
                print( "set new ip {}".format( session.get( "http://httpbin.org/ip" ).text ) )
                response = session.request( "GET", url, headers = headers, params = querystring )
                continue

        if response.text == "{\"stat\":\"很抱歉，沒有符合條件的資料!\"}":
            print(  '很抱歉，沒有符合條件的資料 證交所借券系統與證商/證金營業處所借券餘額合計表 {}'.format( date_str ) )
            return True

        print( response.text )

        self.obj = json.loads( response.text )
        self.date = self.obj[ 'date' ]

        self.df = pd.DataFrame( self.obj[ 'data' ][ 0: -3 ], columns = self.obj[ 'fields' ] )

        del self.df[ '借券餘額市值單位：元(6)=(4)*(5)' ]
        del self.df[ '前日借券餘額(1)股' ]
        del self.df[ '市場別' ]
        del self.df[ '本日收盤價(5)單位：元' ]

        self.df[ '借券餘額股' ] = self.df[ '本日借券餘額股(4)=(1)+(2)-(3)' ].str.replace( ',', '' )
        self.df[ '借券餘額異動股借券' ] = self.df[ '本日異動股借券(2)' ].str.replace( ',', '' ).astype( int )
        self.df[ '借券餘額異動股還券' ] = self.df[ '本日異動股還券(3)' ].str.replace( ',', '' ).astype( int )
        self.df[ '股票代號' ] = self.df[ '證券代號' ]

        del self.df[ '證券代號' ]
        del self.df[ '本日借券餘額股(4)=(1)+(2)-(3)' ]
        del self.df[ '本日異動股借券(2)' ]
        del self.df[ '本日異動股還券(3)' ]

        self.df[ '借券餘額差值' ] = self.df[ '借券餘額異動股借券' ] - self.df[ '借券餘額異動股還券' ]

        return False
        # print( self.df )

class Lend:

    def __init__(self):

    # 取得限制日期
        self.datelst = [ ]
        self.df = pd.DataFrame( )

        self.date = ''
        self.path = ''
        self.obj = json.loads( 'NaN' )

    # 分類
    # 借券賣出 當日餘額

    # 下載CSV
    def GetDate( self, date_str ):

        print( 'Lend     Get Date {} 信用額度總量管制餘額表'.format( date_str ) )

        url = "http://www.twse.com.tw/exchangeReport/TWT93U"
        querystring = { "response": "json", "date": date_str, "_": "1520856196380" }
        headers = { 'accept': "application/json, text/javascript, */*; q=0.01", 'x-requested-with': "XMLHttpRequest",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
            'referer': "http://www.twse.com.tw/zh/page/trading/exchange/TWT93U.html",
            'accept-encoding': "gzip, deflate", 'accept-language': "zh-TW",
            'cookie': "_ga=GA1.3.1838061013.1520518370; _gid=GA1.3.1972994029.1520761897; JSESSIONID=627854663D0CFAB13435F0FA8680B206",
            'cache-control': "no-cache" }

        response = ''

        while response == '':
            try:
                response = requests.request( "GET", url, headers = headers, params = querystring )
            except requests.exceptions.ConnectionError as e:
                set_new_ip( )
                session = get_tor_session( )
                print( "set new ip {}".format( session.get( "http://httpbin.org/ip" ).text ) )
                response = session.request( "GET", url, headers = headers, params = querystring )
                continue

        # print( response.text )
        self.obj = json.loads( response.text )

        self.date = self.obj[ 'date' ]
        margin = dict( self.obj[ 'groups' ][ 0 ] )
        lend = dict( self.obj[ 'groups' ][ 1 ] )

        # print( lend[ 'title' ] )
        # print( lend[ 'span' ] )
        # print( lend[ 'start' ] )
        # print( obj[ 'stat' ] )
        # print( obj[ 'date' ] )

        column = [ ]

        for val in range( margin[ 'start' ], margin[ 'start'] + margin[ 'span' ] ):
            column.append( margin[ 'title' ] + self.obj[ 'fields' ][ val ] )

        for val in range( lend[ 'start' ], lend[ 'start'] + lend[ 'span' ] ):
            column.append( lend[ 'title' ] + self.obj[ 'fields' ][ val ] )

        column.insert( 0, '股票代號' )
        column.insert( 1, '股票名稱' )
        column.append(  '備註' )

        # print( column, len( column ) )

        self.df = pd.DataFrame( self.obj[ 'data' ][ 0 : -1 ], columns = column )

        del self.df[ '股票名稱' ]
        del self.df[ '融券前日餘額' ]
        del self.df[ '融券賣出' ]
        del self.df[ '融券買進' ]
        del self.df[ '融券現券' ]
        del self.df[ '融券今日餘額' ]
        del self.df[ '融券限額' ]
        del self.df[ '備註' ]
        del self.df[ '借券賣出當日調整' ]
        del self.df[ '借券賣出前日餘額' ]

        self.df[ '借券賣出當日賣出' ] = self.df[ '借券賣出當日賣出' ].str.replace( ',', '' ).astype( int )
        self.df[ '借券賣出當日還券' ] = self.df[ '借券賣出當日還券' ].str.replace( ',', '' ).astype( int )
        self.df[ '借券賣出當日餘額' ] = self.df[ '借券賣出當日餘額' ].str.replace( ',', '' ).astype( int )
        self.df[ '借券賣出今日可限額' ] = self.df[ '借券賣出今日可限額' ].str.replace( ',', '' ).astype( int )
        self.df[ '借券賣出當日差值' ] = self.df[ '借券賣出當日賣出' ] - self.df[ '借券賣出當日還券' ]

        # print( self.df )

def set_new_ip(  ):
    """Change IP using TOR"""
    with Controller.from_port( port = 9051 ) as controller:
        controller.authenticate( )
        controller.signal( Signal.NEWNYM )


def get_tor_session():
    session = requests.session()
    # Tor uses the 9050 port as the default socks port
    session.proxies = {'http':  'socks5://127.0.0.1:9150',
                       'https': 'socks5://127.0.0.1:9150'}

    session.verify = False

    return session

def SaveCSV( df, path ):

    lst = [ '股票代號', '證券名稱',
            '借券餘額股', '借券餘額異動股借券', '借券餘額異動股還券', '借券餘額差值',
            '借券賣出當日餘額', '借券賣出當日賣出', '借券賣出當日還券',
            '借券賣出當日差值', '借券賣出今日可限額' ]

    df = df[ lst ]

    df.to_csv( path, sep = ',', encoding = 'utf-8' )

def GetFile( BdateObj, EdateObj ):

    while BdateObj > EdateObj:

        date = BdateObj.strftime( '%Y%m%d' )
        file_name = '捉取借卷_{}.csv'.format( date )
        weekday = BdateObj.weekday( )
        BdateObj = BdateObj - timedelta( days = 1 )

        if os.path.isfile( file_name ):
            print( "{} 已存在".format( file_name ) )
            continue

        # 禮拜日不捉，因禮拜六有可能補班
        if weekday > 4:
            print( '{} 星期 {} 假日跳過'.format( date, weekday ) )
            continue

        # 借卷餘額
        time.sleep( 0.1 )
        LendOverObj = LendOver(  )
        if LendOverObj.GetData( date ):
            continue

        # 借卷
        time.sleep( 0.1 )
        LendObj = Lend( )
        LendObj.GetDate( date )

        # 合併
        result = pd.merge( LendOverObj.df, LendObj.df, on = '股票代號' )
        SaveCSV( result, file_name )
        print( '{} {} 捉取成功'.format( date, BdateObj.weekday( ) ) )

def main( ):

        BdateObj = datetime.today( )
        EdateObj = datetime.today( ) - timedelta( days = 365 )

        print( 'Data 開始 {} 結束 {}'.format( BdateObj, EdateObj ) )

        GetFile( BdateObj, EdateObj )


if __name__ == '__main__':

    start_tmr = time.time( )

    main( )

    print( 'The script took {:06.2f} minute !'.format( (time.time( ) - start_tmr ) / 60 ) )