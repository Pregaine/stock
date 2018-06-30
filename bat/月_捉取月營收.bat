@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/


REM 設定 work dir
cd /d C:\workspace\stock\03_Month process\

ECHO 執行捉取月營收

PAUSE

C:\Env_Py34\Scripts\python.exe "C:\workspace\stock\03_Month process\捉取營收.py"

ECHO 捉取月營收結束

PAUSE