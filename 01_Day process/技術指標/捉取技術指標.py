import requests
import numpy as np
import pandas as pd
import talib
import pyodbc
import logging
from datetime import datetime, timedelta
import threading
import math
import time
from bs4 import BeautifulSoup as BS
import os
import time

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

class Technical_Indicator:
    """ 輸入線型周期, 查詢股價
        D 日線
        W 週線
        M 月線
        A 還原日線
        5  5分鐘
        10 10分鐘
        30 30分鐘 
        60 60分鐘
        """
    def __init__( self, num = '2497', item = 'D' , **d ):

        path = { '60': num + '_60分線技術指標.csv',
                 'D' : num + '_日線技術指標.csv',
                 'W' : num + '_周線技術指標.csv',
                 'M' : num + '_月線技術指標.csv' }

        self.path = path[ item ]
        self.number = num
        self.df = pd.DataFrame( )
        self.type = item
        self.days = d[ item ]

    def GetDF( self ):

        #  範例 http://jsinfo.wls.com.tw/Z/ZC/ZCW/CZKC1.djbcd?a=2889&b=D&c=1440
        url = "http://jsinfo.wls.com.tw/z/BCD/czkc1.djbcd"

        querystring = { "a": self.number, "b": self.type, "c": self.days, "E": "1", "ver": "5" }

        headers = {
            'if-modified-since': "Wed, 15 Nov 1995 04:58:08 GMT",
            'user-agent': "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101",
            'content-type': "text/html;charset=big5",
            'accept': "*/*",
            'referer': "http://jsinfo.wls.com.tw/z/zc/zcw/zcw1_2330.djhtm",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh-CN;q=0.8,zh;q=0.6,en-US;q=0.4,en;q=0.2",
            'cookie': "_ga=GA1.3.732915069.1501683922; _gid=GA1.3.1336941621.1503823694; _ga=GA1.3.732915069.1501683922; _gid=GA1.3.1336941621.1503823694; _gat=1",
            'cache-control': "no-cache",
            'postman-token': "56cb1330-d546-27a3-01bb-6deab90ed563"
        }

        response = requests.request( "GET", url, headers = headers, params = querystring )

        data = response.text.split( ' ' )

        val = 0
        d = { 1:'日期', 2:'開盤', 3:'最高', 4:'最低', 5:'收盤', 6:'成交量' }

        for i in data:

            val += 1
            lst = i.split( ',' )

            if val == 1:
                self.df[ '日期' ] = lst

            elif val in d.keys( ):
                self.df[ d[ val ] ] = [ float( i ) for i in lst ]

        if self.type is not '60':
            self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%Y/%m/%d" )

        return False

    def CombineDF( self ):

        try:
            df_read = pd.read_csv( self.path, sep = ',', encoding = 'utf8', false_values = 'NA', dtype={ '日期': str } )
            df_read = df_read[ [ '日期', '開盤', '最高', '最低', '收盤', '成交量'  ] ]

            if self.type is not '60':
                df_read[ '日期' ] = pd.to_datetime( df_read[ '日期' ], format = "%y%m%d" )
            else:
                df_read[ '日期' ] = pd.to_datetime( df_read[ '日期' ], format = "%y%m%d%H" )

            self.df = pd.concat( [ df_read, self.df ], ignore_index = True )

            self.df.drop_duplicates( [ '日期' ], keep = 'last', inplace = True )
            self.df.sort_values( by = '日期',  ascending=True, inplace = True )
            self.df.reset_index( drop = True, inplace = True )

        except Exception as e:
            pass
            # print( str( e ) )
            # logging.exception( e )
            # print( self.path, '首次存檔' )

    # Money Flow Index and Ratio
    def MFI( self, n ):

        PP = ( self.df[ '最高' ] + self.df[ '最低' ] + self.df[ '收盤' ] ) / 3
        i = 0
        PosMF = [ 0 ]
        NegMF = [ 0 ]

        while i < self.df.index[ -1 ]:

            if PP[ i + 1 ] > PP[ i ]:
                PosMF.append( PP[ i + 1 ] * self.df.get_value( i + 1, '成交量' ) )
            else:
                PosMF.append( 0 )

            if PP[ i + 1 ] > PP[ i ]:
                NegMF.append( 0 )
            else:
                NegMF.append( PP[ i + 1 ] * self.df.get_value( i + 1, '成交量' ) )

            i = i + 1

        NegMF = pd.Series( NegMF )
        PosMF = pd.Series( PosMF )

        NegMF = NegMF.rolling( center = False, window = n ).mean( )
        PosMF = PosMF.rolling( center = False, window = n ).mean( )

        MFR = pd.Series( PosMF / NegMF )

        self.df[ 'MFI' + str( n ) ] = 100 - ( 100 / ( 1 + MFR ) )

    def GetMA( self, lst ):

        C = np.array( self.df[ '收盤' ], dtype = float, ndmin = 1 )

        for val in lst:
            self.df[ 'MA' + str( val ) ] = talib.SMA( C, val )

    def GetRSI(self, lst ):

        C = np.array( self.df[ '收盤' ], dtype = float, ndmin = 1 )

        for val in lst:
            self.df[ 'RSI' + str( val ) ] = talib.RSI( C, timeperiod = val )

    def GetMFI(self, lst ):

        for val in lst:
            self.MFI( val )

    def GetMACD( self, SHORTPERIOD = 12, LONGPERIOD = 26, SMOOTHPERIOD = 9 ):

        C = np.array( self.df[ '收盤' ], dtype = float, ndmin = 1 )
        H = np.array( self.df[ '最高' ], dtype = float, ndmin = 1 )
        L = np.array( self.df[ '最低' ], dtype = float, ndmin = 1 )

        # ------ MACD Begin. ----------------------------
        # 使用MACD需要设置长短均线和macd平均线的参数
        # 用Talib计算MACD取值，得到三个时间序列数组，分别为macd,signal 和 hist
        DIF = ( H + L + 2 * C ) / 4

        dif_str = 'MACD DIF' + str( SHORTPERIOD )
        dem_str = 'DEM' + str( LONGPERIOD )
        osc_str = 'OSC' + str( SHORTPERIOD ) + ',' + str( LONGPERIOD ) + ',' + str( SMOOTHPERIOD )

        self.df[ dif_str ], self.df[ dem_str ], self.df[ osc_str ] = talib.MACD( DIF, SHORTPERIOD, LONGPERIOD,
                                                                                SMOOTHPERIOD )


        # ------ MACD End. ------------------------------

    def GetWR( self, lst ):

        C = np.array( self.df[ '收盤' ], dtype = float, ndmin = 1 )
        H = np.array( self.df[ '最高' ], dtype = float, ndmin = 1 )
        L = np.array( self.df[ '最低' ], dtype = float, ndmin = 1 )

        for val in lst:
            self.df[ 'WILLR' + str( val ) ] = talib.WILLR( H, L, C, timeperiod = val )

    def GetKD( self, period = 9, k = 3, d = 3 ):

        dic = { 3:'3', 9:'9', 2:'2' }
        # *************** KD 指標 Begin. ****************************#
        kdj_n = period

        # 计算N日内的high和low，需要滚动计算\n",
        self.df[ 'lown' ] = self.df[ '最低' ].rolling( center = False, window = kdj_n ).min( )
        self.df[ 'lown' ].fillna( value = self.df[ '最低' ].expanding( min_periods = 1 ).min( ), inplace = True )
        self.df[ 'highn' ] = self.df[ '最高' ].rolling( center = False, window = kdj_n ).max( )
        self.df[ 'highn' ].fillna( value = self.df[ '最高' ].expanding( min_periods = 1 ).max( ), inplace = True )

        self.df[ 'rsv' ] = ( self.df[ '收盤' ] - self.df[ 'lown' ] ) / (self.df[ 'highn' ] - self.df[ 'lown' ]) * 100

        str_k = 'K' + dic[ kdj_n ] + ',' + dic[ k ]
        str_d = 'D' + dic[ kdj_n ] + ',' + dic[ d ]

        # 计算K值
        self.df[ str_k ] = self.df[ 'rsv' ].ewm( ignore_na = False, adjust = True, com = ( k -1 ), min_periods = 0 ).mean( )

        # 计算D值,
        self.df[ str_d ] = self.df[ str_k ].ewm( ignore_na = False, adjust = True, com = ( d - 1 ), min_periods = 0 ).mean( )

        # 计算J值
        # self.df[ 'kdj_j' ] = 3 * self.df[ 'kdj_k' ] - 2 * self.df[ 'kdj_d' ]

        self.df.drop( 'lown', axis = 1, level = None, inplace = True )
        self.df.drop( 'highn', axis = 1, level = None, inplace = True )
        self.df.drop( 'rsv', axis = 1, level = None, inplace = True )
        # *************** KD 指標 End. ******************************

    def GetTi( self ):

        C = np.array( self.df[ '收盤' ], dtype = float, ndmin = 1 )
        H = np.array( self.df[ '最高' ], dtype = float, ndmin = 1 )
        L = np.array( self.df[ '最低' ], dtype = float, ndmin = 1 )

        # -------- Average Directional Movement Index Begin . --------
        self.df[ 'PLUS_DI' ] = talib.PLUS_DI( H, L, C, timeperiod = 14 )
        self.df[ 'MINUS_DI' ] = talib.MINUS_DI( H, L, C, timeperiod = 14 )
        self.df[ 'DX' ] = talib.DX( H, L, C, timeperiod = 14 )
        self.df[ 'ADX' ] = talib.ADX( H, L, C, timeperiod = 14 )
        # ------- Average Directional Movement Index End . --------

        # -------- Bollinger Bands Begin. --------
        # 布林 是 OK，但倒過來

        self.df[ 'tmpMA20'  ] = talib.SMA( C, 20 )
        self.df[ 'tmpMA60' ] = talib.SMA( C, 60 )

        self.df[ 'Upperband' ], self.df[ 'Middleband' ], self.df[ 'Dnperband' ] = talib.BBANDS( C, timeperiod = 20,
                                                                                                nbdevup = 2,
                                                                                                nbdevdn = 2,
                                                                                                matype = 0 )
        self.df[ '%BB' ] = ( C - self.df[ 'Dnperband' ] ) / ( self.df[ 'Upperband' ] - self.df[ 'Dnperband' ] )
        self.df[ 'W20' ] = ( self.df[ 'Upperband' ] - self.df[ 'Dnperband' ] ) / self.df[ 'tmpMA20' ]
        # -------- Bollinger Bands Begin. --------

        # ---------------- 乖離 指標 Begin. ------------------------
        # 乖離 OK, 但比較是倒過來
        # 20 Bias=(C-SMA20)/SMA20
        # 60 Bias=(C-SMA60)/SMA60
        self.df[ '20 Bias' ] = ( C - self.df[ 'tmpMA20' ]) / self.df[ 'tmpMA20' ]
        self.df[ '60 Bias' ] = ( C - self.df[ 'tmpMA60' ]) / self.df[ 'tmpMA60' ]

        self.df.drop( 'tmpMA60', axis = 1, level = None, inplace = True )
        self.df.drop( 'tmpMA20', axis = 1, level = None, inplace = True )
        # ---------------- 乖離 指標 End. ------------------------

        self.df = self.df.iloc[ ::-1 ]

    def PCT_Change( self, df_w, df_m ):

        tmp_w_lst = [ ]
        tmp_m_lst = [ ]

        for date in self.df[ '日期' ]:

            now_price = self.df[ self.df[ '日期' ] == date ][ '收盤' ].values[ 0 ]

            try:
                week_close_price = df_w[ df_w[ '日期' ] <= date ][ '收盤' ].values[ -2 ]
                tmp_w_lst.append( (now_price - week_close_price) / week_close_price * 100 )
            except IndexError:
                tmp_w_lst.append( None )

            try:
                month_close_price = df_m[ df_m[ '日期' ] <= date ][ '收盤' ].values[ -2 ]
                tmp_m_lst.append( ( now_price - month_close_price ) / month_close_price * 100 )
            except IndexError:
                tmp_m_lst.append( None )

        self.df[ '日漲幅' ] = self.df[ '收盤' ].pct_change( 1 ) * 100
        self.df[ '周漲幅' ] = tmp_w_lst
        self.df[ '月漲幅' ] = tmp_m_lst

    # TODO 對方網站改變60分線取消月字串，導致錯誤 18/03/03
    def ConverYearLst( self ):

        now = datetime.now( )
        now_month = now.month
        now_year  = now.year

        # TODO 有沒有更好的方法
        index = self.df.index[ -1 ]

        try:
            pre_day = int( self.df.loc[ index, '日期' ][ 0:2 ] )
        except ValueError as e:
            # print( str(e) )
            print( "股號", self.number, self.type, '資料不足' )
            return True

        for val in range( self.df.index[ -1 ], -1, -1 ):

            now_day  = int( self.df.loc[ val, '日期' ][ 0:2 ] )

            # print( 'now_year', now_year, 'now_month', now_month, 'now_day', now_day )

            if now_day > pre_day:
                if now_month > 1:
                    now_month =  now_month - 1
                else:
                    now_month = 12
                    now_year  = now_year - 1

            pre_day = now_day

            # print( 'now', now_year, now_month, now_day )
            # print( "self.df.loc[ val, '日期' ]", self.df.loc[ val, '日期' ] )

            self.df.loc[ val, '日期' ] = str( now_year ) + str( now_month ) + self.df.loc[ val, '日期' ][ 0:4 ]
            # print( now_year, '年',  now_month, '月', now_day, '日' )
            # print( self.df.loc[ val, '日期' ] )
            # print( self.df.loc[ val, '日期' ] )

        return False
        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%Y%m%d%H" )

    def SaveCSV( self ):

        # cols = [ '券商', '日期', '買進均價', '賣出均價', '買進張數', '賣出張數', '買賣超張數', '買賣超股數', '買進價格*張數', '賣出價格*張數',
        #          '買賣超金額', '買賣超佔股本比', '股本', '收盤', '成交量' ]
        # self.df_d.reindex( columns = cols )

        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )

        if self.type is not '60':
            self.df.to_csv( self.path, sep = ',', encoding = 'utf-8', date_format='%y%m%d' )
        else:
            self.df.to_csv( self.path, sep = ',', encoding = 'utf-8', date_format='%y%m%d%H' )

def _GetMonth( obj ):

    obj.GetDF( )

    obj.CombineDF( )

    obj.GetMA( [ 3, 6, 12, 24, 36, 60, 120 ] )
    obj.GetRSI( [ 2, 5, 10 ] )
    obj.GetKD( period = 9, k = 3, d = 3 )
    obj.GetKD( period = 3, k = 2, d = 3 )
    obj.GetMFI( [ 4, 6, 14 ] )
    obj.GetMACD( SHORTPERIOD = 6, LONGPERIOD = 12, SMOOTHPERIOD = 9 )
    obj.GetMACD( SHORTPERIOD = 12, LONGPERIOD = 26, SMOOTHPERIOD = 9 )
    obj.GetWR( [ 9, 18, 42, 14, 24, 56, 72 ] )

    obj.GetTi( )
    obj.SaveCSV( )

def _GetWeek( obj ):

    obj.GetDF( )

    obj.CombineDF( )

    obj.GetMA( [ 4, 12, 24, 48, 96, 144, 240, 480 ] )
    obj.GetRSI( [ 2, 3, 4, 5, 10 ] )
    obj.GetKD( period = 9, k = 3, d = 3 )
    obj.GetKD( period = 3, k = 2, d = 3 )
    obj.GetMFI( [ 4, 6, 14 ] )
    obj.GetMACD( SHORTPERIOD = 6, LONGPERIOD = 12, SMOOTHPERIOD = 9 )
    obj.GetMACD( SHORTPERIOD = 12, LONGPERIOD = 26, SMOOTHPERIOD = 9 )
    obj.GetWR( [ 9, 18, 42, 14, 24, 56, 72 ] )

    obj.GetTi( )
    obj.SaveCSV( )

def _GetDay( obj, week, month ):

    obj.GetDF( )

    obj.CombineDF( )
    obj.PCT_Change( week.df, month.df )

    obj.GetMA( [ 3, 5, 8, 10, 20, 60, 120, 240, 480, 600, 840, 1200 ] )
    obj.GetRSI( [ 2, 4, 5, 10 ] )
    obj.GetKD( period = 9, k = 3, d = 3 )
    obj.GetKD( period = 3, k = 2, d = 3 )
    obj.GetMFI( [ 4, 6, 14 ] )
    obj.GetMACD( SHORTPERIOD = 6, LONGPERIOD = 12, SMOOTHPERIOD = 9 )
    obj.GetMACD( SHORTPERIOD = 12, LONGPERIOD = 26, SMOOTHPERIOD = 9 )
    obj.GetWR( [ 9, 18, 42, 14, 24, 56, 72 ] )

    obj.GetTi( )
    obj.SaveCSV( )

def _Get60Minute( obj ):

    obj.GetDF( )

    if obj.ConverYearLst( ):
        return True

    obj.CombineDF( )

    obj.GetMA( [ 25, 50, 100, 300, 600, 1200 ] )
    obj.GetRSI( [ 2, 4, 5, 10 ] )
    obj.GetKD( period = 9, k = 3, d = 3 )
    obj.GetKD( period = 3, k = 2, d = 3 )
    obj.GetMFI( [ 5, 6, 14 ] )
    obj.GetMACD( SHORTPERIOD = 6, LONGPERIOD = 12, SMOOTHPERIOD = 9 )
    obj.GetMACD( SHORTPERIOD = 12, LONGPERIOD = 26, SMOOTHPERIOD = 9 )
    obj.GetWR( [ 9, 18, 42, 14, 24, 56, 72 ] )

    obj.GetTi( )
    obj.SaveCSV( )

def DetStockIsNotExist( number ):

    url = "http://jsinfo.wls.com.tw/z/zc/zcw/zcw1_{}.djhtm".format( number )

    headers = { 'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'accept-encoding': "gzip, deflate", 'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        'cookie': "_ga=GA1.3.1434115831.1520137318; _gid=GA1.3.2080379860.1520137318; _uetsid=_uetf7314cc5",
        'cache-control': "no-cache", 'postman-token': "123a262f-8920-74b0-c4da-b2e950cc49c5" }

    response = requests.request( "GET", url, headers = headers )

    soup = BS( response.text, "html.parser" )

    print( '股號', number, '處理中' )

    try:
        row = soup.find( 'center' ).text
    except AttributeError as e:
        # print( str( e ) )
        return False

    if row == '所選個股代碼錯誤':
        print( row )
        return True
    else:
        return  False

def GetFile( *lst ):

    query = { 'W': 480, 'D': 1200, 'M': 120, '60': 1200 }

    for stock in lst:
        time.sleep( 0.1 )
        start_tmr = datetime.now( )

        ti_W  = Technical_Indicator( stock, 'W', **query )
        ti_M  = Technical_Indicator( stock, 'M', **query )
        ti_60 = Technical_Indicator( stock, '60', **query )
        ti_D  = Technical_Indicator( stock, 'D', **query )

        #  print( '股號', stock, ' ', end="" )
        if DetStockIsNotExist( stock ):
            continue

        # try:
        if _Get60Minute( ti_60 ):
            continue
        # except:
        # print( stock, '捉取60分線發生問題' )

        # try:
        _GetWeek( ti_W )
        # except:
        #     print( stock, '周線無資料' )

        # try:
        _GetMonth( ti_M )
        # except:
        #     print( stock, '捉取月線發生問題' )

        # try:
        _GetDay( ti_D, ti_W, ti_M )
        # except:
        # print( stock, "捉取日線發生問題" )

        consumption = datetime.now( ) - start_tmr
        print( '股號', stock, '花費時間', consumption )

def main( ):
    """國票證券 技術分析爬蟲"""

    server   = 'localhost'
    database = 'StockDB'
    username = 'sa'
    password = 'admin'

    try:
        db = dbHandle( server, database, username, password )
    except Exception as e:
        print( str(e) )
        db = dbHandle(server, database, username, '292929' )

    stock_lst = db.GetStockList( )
    one_days_ago = datetime.now( ) - timedelta( days = 1 )

    for file in stock_lst:
        try:
            path = file + '_60分線技術指標.csv'
            filetime = datetime.fromtimestamp( os.path.getctime( path ) )
            if filetime < one_days_ago:
                pass
            else:
                print( path, '檔案更新時間不超過1天' )
                stock_lst.remove( file )
        except FileNotFoundError:
            pass

    print( '股票此次更新', len(stock_lst), '數量' )

    # stock_lst = stock_lst[ 500: ]

    """股票已下市"""
    # stock_lst = [ '3559', '0050', '0058', '0053' ]
    # stock_lst = [ '0050', '3312' ]
    # GetFile( stock_lst )

    thread_count = 2
    thread_list = [ ]

    for i in range( thread_count ):
        start = math.floor( i * len( stock_lst ) / thread_count ) + 1
        end = math.floor( (i + 1) * len( stock_lst ) / thread_count ) + 1
        thread_list.append( threading.Thread( target = GetFile, args = stock_lst[ start:end ]  ) )

    for thread in thread_list:
        thread.start( )

    for thread in thread_list:
        thread.join( )

if __name__ == '__main__':

    start_time = time.time( )

    main( )

    print( 'The script took {:06.2f} minute !'.format( ( time.time( ) - start_time ) / 60  ) )