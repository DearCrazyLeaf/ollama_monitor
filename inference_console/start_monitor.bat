@echo off
setlocal
cd /d "%~dp0"
echo Starting Ollama Inference Monitor...
if not exist ".venv\Scripts\python.exe" (
  py -3.12 -m venv .venv
  if errorlevel 1 py -3 -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Python 3 was not found. Install Python 3.10+ and enable the Python Launcher.
    pause
    exit /b 1
  )
)
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto :error
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto :error
".venv\Scripts\python.exe" app.py
goto :end
:error
echo.
echo [ERROR] Setup or startup failed. See the message above.
pause
:end
endlocal
