@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:\workspace\stock\01_Day process\技術指標

C:\Env_Py34\Scripts\python.exe "C:\workspace\stock\01_Day process\技術指標\寫入技術指標.py"

ECHO 執行寫入技術指標

PAUSE