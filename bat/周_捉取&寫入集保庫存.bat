@ECHO OFF

REM 設定python work dir
set PYTHONPATH=C:/workspace/stock/

REM 設定 work dir
cd /d C:/workspace/stock/02_Week process/

C:\Env_Py34\Scripts\python.exe "C:/workspace/stock/02_Week process/捉取&寫入集保庫存.py"

ECHO 執行捉取&寫入集保庫存

PAUSE