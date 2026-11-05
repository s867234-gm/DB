@echo off
chcp 65001 >nul
echo ======================================================================
echo   A_DB 停用并卸载 Windows 自动定时更新任务
echo ======================================================================
python scripts\manage_tasks.py uninstall
pause
