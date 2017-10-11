import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import os

class LendOver:

    def __init__(self):
        # 取得限制日期
        self.datelst = [ ]

        self.df = pd.DataFrame( )

        self.date = ''
        self.path = ''
        self.obj = json.loads( 'NaN' )


    def GetData( self, date_str ):

        url = "http://www.twse.com.tw/exchangeReport/TWT72U"

        # querystring = {"response":"json","date":"20170913","selectType":"SLBNLB","_":"1505545708340"}

        querystring = { "response": "json", "date": date_str, "selectType": "SLBNLB", "_": "1505545708340" }

        headers = {
            'accept': "application/json, text/javascript, */*; q=0.01",
            'x-requested-with': "XMLHttpRequest",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.91 Safari/537.36",
            'referer': "http://www.twse.com.tw/zh/page/trading/exchange/TWT72U.html",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
            'cookie': "JSESSIONID=8688F6ED6AD677B886011F56B3E86D0E; _ga=GA1.3.1953932169.1504347589; _gid=GA1.3.981495820.1505442422",
            'cache-control': "no-cache",
            'postman-token': "6790b24b-e9c5-61ac-5eea-f8ee81d4204e"
            }

        response = requests.request("GET", url, headers=headers, params=querystring)

        # print(response.text)

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

        # print( self.df )

class Lend:

    def __init__(self):

    # 取得限制日期
        self.datelst = [ ]

        self.df = pd.DataFrame( )

    # datetime.datetime( 2012, 3, 23, 23, 24, 55, 173504 )
    # datetime.datetime.today( ).weekday( )

        self.date = ''
        self.path = ''
        self.obj = json.loads( 'NaN' )

    # 分類
    # 借券賣出 當日餘額

    # def GetNowTime(self):

    # 下載CSV
    def GetDate( self, date_str ):

        url = "http://www.twse.com.tw/exchangeReport/TWT93U"

        # querystring = { "response": "json", "date": "20170914", "_": "1505464008724" }

        querystring = { "response": "json", "date": date_str, "_": "1505464008724" }

        headers = \
        {
            'accept': "application/json, text/javascript, */*; q=0.01",
            'x-requested-with': "XMLHttpRequest",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.91 Safari/537.36",
            'referer': "http://www.twse.com.tw/zh/page/trading/exchange/TWT93U.html",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
            'cookie': "_gat=1; _ga=GA1.3.1953932169.1504347589; _gid=GA1.3.981495820.1505442422; JSESSIONID=04A88BEC726B014955739B03A8A87453; JSESSIONID=F232D8F258218AE56423D4075FD5943B; _ga=GA1.3.1953932169.1504347589; _gid=GA1.3.981495820.1505442422",
            'cache-control': "no-cache",
            'postman-token': "4f5c84e9-c7a7-64ba-fdf0-ad96a614614b"
        }

        response = requests.request( "GET", url, headers = headers, params = querystring )

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

def SaveCSV( df, path ):

    lst = [ '股票代號', '證券名稱',
            '借券餘額股', '借券餘額異動股借券', '借券餘額異動股還券', '借券餘額差值',
            '借券賣出當日餘額', '借券賣出當日賣出', '借券賣出當日還券',
            '借券賣出當日差值', '借券賣出今日可限額' ]

    df = df[ lst ]

    df.to_csv( path, sep = ',', encoding = 'utf-8' )

def main( ):

    start_tmr = datetime.now( )

    EdateObj = datetime.today( ) - timedelta( days = 365 )
    BdateObj = datetime.today() + timedelta( days = 1 )

    while BdateObj > EdateObj:

        BdateObj = BdateObj - timedelta( days = 1 )

        weekday = BdateObj.weekday( )
        date = BdateObj.strftime( '%Y%m%d' )

        # 六, 日不捉
        if weekday > 5:
            continue

        file_name = '捉取借卷_' + date + '.csv'

        if os.path.isfile( file_name ):
            print( file_name, "已存在" )
            continue

        try:
            # 借卷
            LendObj = Lend( )
            LendObj.GetDate( date )

            # 借卷餘額
            LendOverObj = LendOver(  )
            LendOverObj.GetData( date )

            # 合併
            result = pd.merge( LendOverObj.df, LendObj.df, on = '股票代號' )

            SaveCSV( result, file_name )

            print( date, BdateObj.weekday( ) )
            # print( result.iloc[ -1 ] )

        except:
            print( date, weekday, "借卷捉取網頁失敗" )

    print( datetime.now( ) - start_tmr )


if __name__ == '__main__':

    main( )
