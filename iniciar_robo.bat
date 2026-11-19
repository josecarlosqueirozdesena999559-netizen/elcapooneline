@echo off
title Robo BullEx ORIUN - IA Autonoma
chcp 65001 > nul
cls
echo ===================================================
echo     INICIANDO ROBO BULLEX ORIUN (IA AUTONOMA)
echo ===================================================
echo.
echo Abrindo servidor em http://localhost:8085 ...
echo.

where python >nul 2>nul
if %ERRORLEVEL% equ 0 (
    start http://localhost:8085
    python "%~dp0server.py"
) else (
    if exist "C:\Users\KENEDI\AppData\Local\Programs\Python\Python313\python.exe" (
        start http://localhost:8085
        "C:\Users\KENEDI\AppData\Local\Programs\Python\Python313\python.exe" "%~dp0server.py"
    ) else (
        echo [ERRO] Python nao foi encontrado no sistema!
        echo Instale o Python (versao 3.10 ou superior) em https://www.python.org/
        echo Lembre-se de marcar a opcao "Add Python to PATH" durante a instalacao.
        echo.
        pause
    )
)
pause
