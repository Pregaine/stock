@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:/workspace/stock/01_Day process/融資融券/

C:\Env_Py34\Scripts\python.exe "C:/workspace/stock/01_Day process/融資融券/寫入融資融卷.py"

ECHO 執行捉取融資融卷

PAUSE