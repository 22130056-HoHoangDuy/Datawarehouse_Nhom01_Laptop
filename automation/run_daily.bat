@echo off
cd /d "%~dp0\.."

echo [%date% %time%] === START === >> logs\automation.log

python automation\pipeline.py

echo [%date% %time%] === END === >> logs\automation.log
exit
