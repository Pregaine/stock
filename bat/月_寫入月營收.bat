@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/


REM 設定 work dir
cd /d C:\workspace\stock\03_Month process\

ECHO 寫入月營收

PAUSE

C:\Env_Py34\Scripts\python.exe "C:\workspace\stock\03_Month process\寫入營收.py"

ECHO 寫入月營收結束

PAUSE