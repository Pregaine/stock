# stock


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