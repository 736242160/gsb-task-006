@echo off
chcp 65001 >nul
cd /d "%~dp0"
where py >nul 2>nul && (py -3 gc_stw_sim.py %* & goto :eof)
python gc_stw_sim.py %*