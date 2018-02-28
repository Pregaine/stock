#
Bug‧De‧羅傑被老闆開除前說「想要我的bug嗎？想要的話全部給你，去找吧！我把bug都埋在程式碼裡面！」 後來的新進員工把這Bug稱為「大臭蟲」(One Bug)，為了爭相除去這個Bug， 
許多員工日以繼夜的加班，開啟了「大臭蟲時代」。

## 01_Day process

- 3大法人
    - 爬蟲元大券商網頁       http://jdata.yuanta.com.tw/z/zc/zcl/zcl.djhtm
    - 寫入MSSQL
    
- 技術指標
    - 爬蟲國票證券網頁
    http://jsinfo.wls.com.tw/z/zc/zcw/zcw1_2330.djhtm 
    - 寫入MSSQL
    
- 券商分點
    - 爬蟲買賣日報表查詢系統 http://bsr.twse.com.tw/bshtm/
    - 寫入MSSQL
    - 分析籌碼集中度，從MSSQL查詢資料
    
- 借還券
    - 爬蟲借券餘額合計表 http://www.twse.com.tw/zh/page/trading/exchange/TWT72U.html
    - 爬蟲信用額度總量管制餘額表 http://www.twse.com.tw/zh/page/trading/exchange/TWT93U.html
    - 寫入MSSQL

- 融資融券
    - 爬蟲元大證券融資融券
    http://jdata.yuanta.com.tw/z/zc/zcn/zcn_2330.djhtm
    - 寫入MSSQL

## 02_Week process

- 集保庫存
    - 爬蟲集保庫存 http://www.tdcc.com.tw/smWeb/QryStock.jsp
    - 寫入MSSQL 

## 03_Month process

- 營收
    - 爬蟲公開資訊觀測站 http://mops.twse.com.tw/nas/t21/sii/t21sc03_106_1_0.html
    - 寫入MSSQL
    
##
計算籌碼集中度.py 
- 18/02/28 
    > Init DB Cmd 修正錯誤訊息亂碼，加入SQL Cmd 'SET LANGUAGE us_english;'  
        GetDates Function，修正SQL Cmd 於替代字串加入'符號 
 ``` 
        cmd = 'SELECT TOP ( {0} ) date FROM DailyTrade WHERE stock = \'{1}\' ' \
              'GROUP BY date ORDER BY date desc'.format( days, num )
```
  
 
 
