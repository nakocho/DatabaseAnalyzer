@echo off
cd /d %~dp0

echo Activando entorno virtual...
call venv\Scripts\activate

echo Iniciando la aplicación...
start http://localhost:5000
python main.py