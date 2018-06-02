@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:\workspace\stock\01_Day process\3大法人

ECHO 捉取3大法人

PAUSE

C:\Env_Py34\Scripts\python.exe "C:\workspace\stock\01_Day process\3大法人/捉取3大法人持股.py"

ECHO 捉取3大法人結束

PAUSE