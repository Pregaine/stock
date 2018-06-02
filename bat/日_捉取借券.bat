@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:\workspace\stock\01_Day process\借還券

ECHO 捉取借還卷

PAUSE

C:\Env_Py34\Scripts\python.exe "C:\workspace\stock\01_Day process\借還券/捉取借還卷.py"

ECHO 捉取借還卷結束

PAUSE