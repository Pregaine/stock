@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:/workspace/stock/01_Day process/券商分點/

C:\Env_Py34\Scripts\python.exe "C:/workspace/stock/01_Day process/券商分點/寫入卷商買賣.py"

ECHO 執行寫入卷商買賣

PAUSE