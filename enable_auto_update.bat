@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul
echo ======================================================================
echo   A_DB 启用 Windows 自动定时更新任务
echo   - 盘中 16 个 15m 节点自动增量推进 (09:46 ~ 15:01)
echo   - 盘后 15:32 完整日线与分钟线归档更新
echo ======================================================================
python scripts\manage_tasks.py install
pause
