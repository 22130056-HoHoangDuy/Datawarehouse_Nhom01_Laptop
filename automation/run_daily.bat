@echo off
REM === Chạy pipeline ETL laptop ===
cd /d "%~dp0\.."

echo [%date% %time%] === BẮT ĐẦU CHU TRÌNH === >> logs\automation.log

py automation\pipline.py

echo [%date% %time%] === KẾT THÚC CHU TRÌNH === >> logs\automation.log
exit
