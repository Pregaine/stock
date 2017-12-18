import requests
import json
import pandas as pd
from datetime import datetime, timedelta

# -*- coding: utf-8 -*-

class Handle:

    def __init__(self):

        self.df = pd.DataFrame( )
        self.date = ""

        weekday = datetime.now( ).weekday( )

        if weekday > 4:
             cal_day = datetime.now( ) - timedelta( days = ( weekday - 4 ) )
             day_str = cal_day.strftime( "%Y%m%d" )
        else:
            day_str = datetime.now( ).strftime( "%Y%m%d" )

        day_str = '20171103'

        url = "http://www.tse.com.tw/exchangeReport/MI_INDEX"

        querystring = { "response":"json","date":day_str,"type":"ALLBUT0999","_":"1496850467797" }

        headers = {
            'accept': "application/json, text/javascript, */*; q=0.01",
            'x-requested-with': "XMLHttpRequest",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/58.0.3029.110 Safari/537.36",
            'referer': "http://www.tse.com.tw/zh/page/trading/exchange/MI_INDEX.html",
            'accept-encoding': "gzip, deflate, sdch",
            'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
            'cookie': "JSESSIONID=DD51E68FDCD45847F79E947193947071",
            'postman-token': "1a4da3bb-a377-480f-ad2b-b5e892bc759e"
            }

        response = requests.request( "GET", url, headers=headers, params=querystring )

        obj = json.loads( response.text )

        print( day_str, obj['stat'] )

        self.date = obj[ 'date' ]

        self.df = pd.DataFrame( obj[ 'data5' ], columns = obj[ 'fields5' ] )

        del self.df[ '漲跌(+/-)' ]

    def save_file( self, path ):

        self.df.to_csv( path + self.date + '個股收盤.csv' )

    def get_file( self ):

        return self.df

    def get_data( self ):

        return self.date

    def getlist(self):

        # print( type( self.df[ '證券代號'][ 0 ] ) )

        return self.df[ '證券代號' ].values.tolist( )

def main( ):

    obj = stock_inquire( )

    print( '查詢日期', obj.get_data( ) )

    print( '個股收盤', obj.df )

    print( '個股清單', obj.get_stock_list( ) )

    obj.save_file( ".//" )

if __name__ == '__main__':

    obj = Handle()

    print( obj.get_data( ) )

    print( obj.getlist( ) )