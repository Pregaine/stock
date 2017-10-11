# coding: utf-8

import shutil
import requests
import re
import pandas as pd
import csv
import os, sys
import time
import stock_inquire.stock_inquire as siq


def WriteCsv( filename, lst ):

    with open( filename, 'w', newline='\n', encoding = 'utf8' ) as csv_out_file:

        filewriter = csv.writer( csv_out_file, delimiter = ',' )

        for in_val in lst:
            filewriter.writerow( in_val )

def ReadCsv( filename ):

    with open( filename, 'r', newline = '\n', encoding = 'utf8' ) as csv_file:
        lst = csv.reader( csv_file )

    return lst
#-----------------------------------------------------------------------
#傳入清單,回傳內容無重覆清單
#-----------------------------------------------------------------------
def remove_duplicates( l ):
    return list( set( l ) )
#-----------------------------------------------------------------------

def Det_Dict_OverCnt( in_dict, compare ):

    for i in in_dict.values( ):

        if i < compare:
            return False

    return True

def Resort_List( path, lst ):

    dirs = os.listdir( path )

    for name in dirs:

            i = re.search( '(\d{4,6}[A-Z]{0,3})', name )

            if i is not None:
                if i.group(0) in lst:
                    lst.remove( i.group(0) )

    return lst
    

 
# OutputPath =  sys.argv[ 1 ]
Load_Sucess_List = [ ]
OutputPath = ".\\"
#------------------------------------------------------------------------
#網頁頭參數
#詢問得到以下參數
#viewstate, eventvalidation,
#------------------------------------------------------------------------
headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0.1) Gecko/2010010' \
    '1 Firefox/4.0.1',
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language':'en-us,en;q=0.5',
    'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7'}

rs = requests.session()

res = rs.get( 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx', stream = True, verify = False, headers =headers )

# print( res.cookies )
# print(res.text)

viewstate = re.search( 'VIEWSTATE"\s+value=.*=', res.text )
viewstate = viewstate.group( )[18:]

eventvalidation = re.search( 'EVENTVALIDATION"\s+value=.*\w', res.text )
eventvalidation = eventvalidation.group()[24:]

# encode_viewstate = urllib.parse.urlencode( { viewstate : '' }  )
# print( encode_viewstate[:-1] )
# encode_eventvalidation = urllib.parse.urlencode( { eventvalidation : '' } )
# print( encode_eventvalidation[:-1] )
#------------------------------------------------------------------------

#------------------------------------------------------------------------
#搜尋網頁回應內容關鍵字'CaptchaImage.*guid+\S*\w'
#根據關鍵字獲得驗證碼圖片
#------------------------------------------------------------------------
str = re.search( 'CaptchaImage.*guid+\S*\w', res.text )

res = rs.get( 'http://bsr.twse.com.tw/bshtm/' + str.group( ), stream = True, verify = False ) 

f = open( 'check.png', 'wb' )
shutil.copyfileobj( res.raw, f )
f.close

print( 'http://bsr.twse.com.tw/bshtm/' + str.group() )
#------------------------------------------------------------------------

#------------------------------------------------------------------------
#讀取csv file,得到股號清單
#------------------------------------------------------------------------

a = siq.stock_inquire( )

stock_code_list = a.get_stock_list( )
#------------------------------------------------------------------------


#------------------------------------------------------------------------
#初始化參數
#------------------------------------------------------------------------
num = 0
headers = {'User-Agent': 'Mozilla/5.0'}
timeout_dict = dict( )
resort  = 0
#------------------------------------------------------------------------


#------------------------------------------------------------------------
#根據股號清單,詢問網頁
#------------------------------------------------------------------------
while len( stock_code_list ):
    
    #詢問股號,網頁無回應或回應錯誤次數
    miss_cnt = 0

    date = None

    num = stock_code_list.pop( 0 )

    while date is None and miss_cnt < 2:

        miss_cnt = miss_cnt + 1
        
        time.sleep( 1 )

        rs = requests.session( )

        res = rs.get( 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx', stream = True, verify = False, headers = headers, timeout=None )

        print( '股號', num, 'Response', res.status_code, len( stock_code_list ) )

		#----------------------------------------------------------------------------------
		#根據網頁響應內容"res.text"
		#取出參數viewstate, eventvalidation
        #----------------------------------------------------------------------------------
        try:
            viewstate = re.search( 'VIEWSTATE"\s+value=.*=', res.text )
            viewstate = viewstate.group()[18:]
            eventvalidation = re.search( 'EVENTVALIDATION"\s+value=.*\w', res.text )
            eventvalidation = eventvalidation.group( )[ 24: ]
        except:
            continue
        
		#----------------------------------------------------------------------------------
		#根據網頁響應內容"res.text"
		#根據參數viewstate, eventvalidation
		#得到個股卷商交易資料"date"
        #----------------------------------------------------------------------------------
        key = re.search('CaptchaImage.*guid+\S*\w', res.text )

        res = rs.get( 'http://bsr.twse.com.tw/bshtm/' + key.group(), stream = True, verify = False )


        #f = open('check.png', 'wb')
		#shutil.copyfileobj( res.raw, f )
		#f.close
        #----------------------------------------------------------------------------------
		
        payload = {
        '__EVENTTARGET':'',
        '__EVENTARGUMENT':'',
        '__LASTFOCUS':'',
        '__VIEWSTATE' : viewstate,                      #encode_viewstate[:-1],
        '__EVENTVALIDATION' : eventvalidation,          #encode_eventvalidation[:-1],
        'RadioButton_Normal' : 'RadioButton_Normal',
        'TextBox_Stkno' : '{}'.format( num ),
        'CaptchaControl1 ' : 'Z67YA',
        'btnOK' : '%E6%9F%A5%E8%A9%A2',
        }

        rs.post( "http://bsr.twse.com.tw/bshtm/bsMenu.aspx", data=payload, headers=headers, verify = False, stream = True )
        res = rs.get( 'http://bsr.twse.com.tw/bshtm/bsContent.aspx?v=t',verify = False ,stream = True, )
        date = re.search( 'receive_date.*\s.*\d', res.text )

        #print( '股號', stock_code_list[index], 'Response', res.status_code, '內容' )
        
    #----------------------------------------------------------------------------------    
	#查詢次數已達到20次,未成功切換下檔股號
	#----------------------------------------------------------------------------------
    if miss_cnt == 2:

        if num in timeout_dict:
            timeout_dict[ num ] += 1
        else:
            timeout_dict[ num ] = 1

        stock_code_list.append( num )

        print( '查詢逾時', stock_code_list[ -1 ], '移入屁股' )

        print( timeout_dict.items( ) )

        if Det_Dict_OverCnt( timeout_dict, 5 ):
            print( '不爽查了, 存入Log' )
            del stock_code_list[ : ]
            with open( 'log.csv', 'w' ) as csv_file:
                writer = csv.writer( csv_file )
                for key, value in timeout_dict.items( ):
                    val = "{0}".format( value )
                    writer.writerow( [ key, val ] )

        continue

    if num in timeout_dict.keys( ):
        timeout_dict.pop( num )
	#----------------------------------------------------------------------------------	
	#前面手續為模擬詢問卷商資料過程
	#成功,代表此Sesion可以得到正確響應內容
	#----------------------------------------------------------------------------------
    date = date.group()[-10:].replace('/', '')
    name = re.search( '&nbsp;.*<', res.text )
    name = name.group()[6:-1]
    #----------------------------------------------------------------------------------

	#----------------------------------------------------------------------------------
	#詢問網址"http://bsr.twse.com.tw/bshtm/bsContent.aspx"
	#得到正確卷商交易資料,存到清單"raw"
	#----------------------------------------------------------------------------------
    tmp_csv = rs.get( 'http://bsr.twse.com.tw/bshtm/bsContent.aspx', verify = False ,stream = True )

    if tmp_csv.status_code > 300 or tmp_csv.status_code < 200:
        tmp_csv = rs.get( 'http://bsr.twse.com.tw/bshtm/bsContent.aspx', verify = False ,stream = True )

    # ----------------------------------------------------------------------------------
    # 斷開連結,斷開鎖鍊
    # ----------------------------------------------------------------------------------
    rs.close( )

    data = tmp_csv.text.splitlines( )

    print( data )

    if data[ 0 ] != '券商買賣股票成交價量資訊':
        stock_code_list.append( num )
        print( '查詢資料錯誤', stock_code_list[ -1 ], '移入屁股' )
        continue

    data[ -1 ] = data[ -1 ].replace( ',,', ',' )
    data[ -1 ] = data[ -1 ].replace( ' ', '' )
    data = data[ 3: ]
    rows = list( )

    for row in data:
        if ',,' in row:
            i = re.search( '.*,,', row )
            rows.append( i.group( ).replace( ',,', ''  ) )

            try:
                i = re.search( ',,.*\s$', row )
                rows.append( i.group( ).replace( ',,', '' ) )
            except AttributeError:
                continue
        else:
            print( row )

            tmp = row.split( ',' )

            if len( tmp ) > 5:
                rows.append( ','.join( tmp[ 0:5 ] ) )
                rows.append( ','.join( tmp[ 5: ] ) )
            else:
                rows.append( ','.join( tmp ) )

	#-------------------------------------
	#初始化資料夾名稱
	#-------------------------------------
    Savefiledir = OutputPath + '全台卷商交易資料_' + date + '\\'

	#-------------------------------------
	#若無資料夾,建立資料夾
	#-------------------------------------
    if not os.path.isdir(Savefiledir):
        os.makedirs(Savefiledir)
        
    #-------------------------------------
	#列印即將寫入檔名
	#-------------------------------------
    path_name = Savefiledir + payload['TextBox_Stkno'] + name + '_' + date + '.csv'
    print( path_name )

    # -----------------------------
    # 檢查路徑內檔名比對code_list
    # -----------------------------
    if resort is 0:
        resort = 1
        stock_code_list = Resort_List( Savefiledir, stock_code_list )

	#-------------------------------------
	#根據清單"raw"檔案寫入csv
	#-------------------------------------
    with open( path_name, 'w', newline='\n', encoding='utf-8' ) as file:
        w = csv.writer( file )
        # w.writerow( [ '序號', '券商', '價格', '買進股數', '賣出股數' ] )
        for data in rows:
            s = data.split( ',' )
            w.writerow( s )




