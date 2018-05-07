@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:\workspace\stock\01_Day process\券商分點\

C:\Env_Py34\Scripts\python.exe "C:\workspace\stock\01_Day process\券商分點\計算籌碼集中度.py"

ECHO 執行計算籌碼集中度

PAUSE