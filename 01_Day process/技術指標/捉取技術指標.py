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
import codes.codes as TWSE

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
    def __init__( self, num = '2497', item = 'D', tree = '', **d ):

        path = { '60': '{}_60分線技術指標.csv'.format( num ),
                 'D': '{}_日線技術指標.csv'.format( num ),
                 'W': '{}_周線技術指標.csv'.format( num ),
                 'M': '{}_月線技術指標.csv'.format( num ) }

        self.path = '{}/{}'.format( tree, path[ item ] )
        self.number = num
        self.df = pd.DataFrame( )
        self.type = item
        self.days = d[ item ]

    def GetDF( self ):

        url = "http://5850web.moneydj.com/z/BCD/czkc1.djbcd"

        querystring = { "a": self.number, "b": self.type, "c": self.days, "E": "1", "ver": "5" }

        headers = {
            'user-agent': "Chrome/64.0.3282.186",
            'content-type': "text/html;charset=big5",
            'accept': "*/*",
            'referer': "http://5850web.moneydj.com/z/zc/zcw/zcw1.DJHTM?A={}".format( self.number ),
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
            'cache-control': "no-cache", }

        try:
            response = requests.request( "GET", url, headers = headers, params = querystring )
        except Exception as e:
            print( response.url )

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
            print( str( e ) )
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
            
            # print( 1, now_price, self.df[ self.df[ '日期' ] == date ][ '日期' ] )

            try:
                week_close_price = df_w.loc[ df_w[ '日期' ] <= date, '收盤' ].values[ 1 ]
                tmp_w_lst.append( (now_price - week_close_price) / week_close_price * 100 )
                
                # print( '2------>', week_close_price, df_w[ df_w[ '日期' ] <= date ][ '日期' ].values[ -2 ] )
                
            except IndexError:
                tmp_w_lst.append( None )

            try:
                month_close_price = df_m.loc[ df_m[ '日期' ] <= date, '收盤' ].values[ 1 ]
                tmp_m_lst.append( ( now_price - month_close_price ) / month_close_price * 100 )
            except IndexError:
                tmp_m_lst.append( None )

        self.df[ '日漲幅' ] = self.df[ '收盤' ].pct_change( 1 ) * 100
        self.df[ '周漲幅' ] = tmp_w_lst
        self.df[ '月漲幅' ] = tmp_m_lst

    # TODO 對方網站改變60分線取消月字串，導致錯誤 18/03/03
    def ConverYearLst( self ):

        try:
            index = self.df.index[ -1 ]
            pre_day = int( self.df.loc[ index, '日期' ][ 0:2 ] )
        except ValueError as e:
            print( "股號", self.number, self.type, '資料不足' )
            return True

        """
        now = datetime.now( )
        now_month = now.month
        now_year  = now.year
        # TODO 有沒有更好的方法
        index = self.df.index[ -1 ]

        for val in range( self.df.index[ -1 ], -1, -1 ):

            now_day  = int( self.df.loc[ val, '日期' ][ 6:8 ] )

            # print( 'now_year', now_year, 'now_month', now_month, 'now_day', now_day )

            if now_day > pre_day:
                if now_month > 1:
                    now_month =  now_month - 1
                else:
                    now_month = 12
                    now_year  = now_year - 1

            pre_day = now_day

            # print( "self.df.loc[ val, '日期' ]", self.df.loc[ val, '日期' ] )
            self.df.loc[ val, '日期' ] = str( now_year - 2000 ) + '{:0>2}'.format( now_month ) + self.df.loc[ val, '日期' ][ 6:10 ]
        """

        self.df[ '日期' ] = self.df[ '日期' ].str[ 0:-2 ]
        self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%Y%m%d%H" )

        return False

    def SaveCSV( self ):
        # cols = [ '券商', '日期', '買進均價', '賣出均價', '買進張數', '賣出張數', '買賣超張數', '買賣超股數', '買進價格*張數', '賣出價格*張數',
        #          '買賣超金額', '買賣超佔股本比', '股本', '收盤', '成交量' ]
        # self.df_d.reindex( columns = cols )

        self.df = self.df.replace( [ np.inf, -np.inf ], np.nan )
        self.df = self.df.round( decimals =  2 )

        if self.type is not '60':
            self.df.to_csv( self.path, sep = ',', encoding = 'utf-8', date_format='%y%m%d' )
        else:
            self.df.to_csv( self.path, sep = ',', encoding = 'utf-8', date_format='%y%m%d%H' )

def _GetMonth( obj, update_m ):

    # try:
    #     compare = pd.read_csv( obj.path, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '日期': str } )
    #     compare = compare[ [ '日期', '開盤', '最高', '最低', '收盤', '成交量' ] ]
    #     compare[ '日期' ] = pd.to_datetime( compare[ '日期' ], format = "%y%m%d" )
    #     now = datetime.now( )
    #     delta = now - compare.loc[ 0, '日期' ]
    #
    #     if delta.days > 25:
    #         print( '{:<7} 月線 {:<2} 天前更新'.format( obj.number, delta.days ) )
    #         obj.GetDF( )
    # except FileNotFoundError:

    if update_m is True:
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

def _GetWeek( obj, update_w ):

    if update_w is True:
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

def _Get60Minute( obj, update_h ):

    if update_h is True:
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

def _DetStockIsNotExist( number ):

    url = "http://5850web.moneydj.com/z/zc/zcw/zcw1.DJHTM"

    querystring = { "A": number }

    headers = {
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/64.0.3282.186",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'accept-encoding': "gzip, deflate",
        'accept-language': "zh-TW,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6",
        'cache-control': "no-cache", }

    response = requests.request( "GET", url, headers = headers, params = querystring )
    soup = BS( response.text, "html.parser" )
    print( '股號{:>7} 處理中'.format(number) )

    try:
        row = soup.find( 'center' ).text
    except AttributeError as e:
        return False

    if row == '所選個股代碼錯誤':
        print( row )
        return True
    else:
        return  False

def GetFile( lst, update_m, update_w, update_h ):

    # query = { 'W': 480, 'D': 1200, 'M': 120, '60': 1200 }
    query = { 'W': 2, 'D': 5, 'M': 1, '60': 10 }
    lst = list( lst )
    path = 'C:/workspace/data/技術指標/'

    while lst:

        start_tmr = datetime.now( )
        stock = lst.pop( 0 )
        ti_W  = Technical_Indicator( stock, 'W', path, **query )
        ti_M  = Technical_Indicator( stock, 'M', path, **query )
        ti_60 = Technical_Indicator( stock, '60', path, **query )
        ti_D  = Technical_Indicator( stock, 'D', path, **query )

        try:
            if _Get60Minute( ti_60, update_h ):
                continue
            _GetWeek( ti_W, update_w )
            _GetMonth( ti_M, update_m )
            _GetDay( ti_D, ti_W, ti_M )

        except Exception as e:
            print( '{}'.format( e ) )
            if '{}'.format( e ) != 'day is out of range for month' and stock != '6131':
                lst.append( stock )
                print( 'lst.append({})'.format( stock ) )

        consumption = datetime.now( ) - start_tmr
        print( '股號{0:>7} 耗時{1:>4} Second {2}'.format( stock, consumption.seconds, len( lst ) ) )

def CompareFileModifyTime( path, hour = 6 ):

    one_days_ago = datetime.now( ) - timedelta( hours = hour )
    try:
        filetime = datetime.fromtimestamp( os.path.getmtime( path ) )
        if filetime < one_days_ago:
            print( '{0:<20}修改 {1} 超過 {2} hour'.format( path, filetime.strftime( '%m-%d %H:%M' ), hour ) )
            return False
        else:
            return True
    except Exception as e:
        return False

def main( ):
    """範例http://5850web.moneydj.com/z/zc/zcw/zcw1_3312.djhtm"""
    try:
        db = dbHandle( 'localhost', 'StockDB', 'sa', 'admin' )
    except Exception as e:
        db = dbHandle( 'localhost', 'StockDB', 'sa', '292929' )

    empty_lst = list( TWSE.codes.keys( ) )
    stock_lst = [ ]

    for stock in empty_lst:
        if CompareFileModifyTime( 'C:/workspace/data/技術指標/{0}_日線技術指標.csv'.format( stock ) ):
            continue
        stock_lst.append( stock )

    # stock_lst = [ '4989', '2330', '1711' ]
    print( '股票需更新{}支'.format( len( stock_lst ) ) )

    thread_count = 2
    update_m = False
    update_w = False
    update_h = False
    thread_list = [ ]

    for i in range( thread_count ):
        start = math.floor( i * len( stock_lst ) / thread_count )
        end   = math.floor( ( i + 1 ) * len( stock_lst ) / thread_count )
        thread_list.append( threading.Thread( target = GetFile, args = ( stock_lst[ start:end ], update_m, update_w, update_h  ) ) )

    for thread in thread_list:
        thread.start( )

    for thread in thread_list:
        thread.join( )

if __name__ == '__main__':

    start_time = time.time( )
    main( )
    print( 'The script took {:06.2f} minute !'.format( ( time.time( ) - start_time ) / 60  ) )