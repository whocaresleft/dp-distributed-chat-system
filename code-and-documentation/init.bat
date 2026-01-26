@echo off
setlocal enabledelayedexpansion

echo Select execution environment:"
echo 1- Docker (through docker-compose, will execute all nodes in docker)
echo 2- Local (uses 127.0.0.1 as addresses for local testing)
set /p choice="[1-2]: "

if "%choice%"=="1" (
    set BOOTSTRAP_ADDR=zmq-bootstrap
    set NAMESERVER_ADDR=zmq-nameservice
) else (
    set BOOTSTRAP_ADDR=127.0.0.1
    set NAMESERVER_ADDR=127.0.0.1
)

echo Creating BOOTSTRAP env...
if not exist B0OTSTRAP mkdir B0OTSTRAP
type nul > B0OTSTRAP\bootstrap.log
if not exist B0OTSTRAP\bootstrap.cfg (
    type nul > B0OTSTRAP\bootstrap.cfg
)

echo Creating nodes env... (6 nodes)
set BASE_CPORT=46000
set BASE_DPORT=50000
set BASE_HPORT=8080

for /L %%i in (1,1,6) do (
    set FOLDER=NODE_%%i
    if not exist !FOLDER! mkdir !FOLDER!
    
    set /a CPORT=BASE_CPORT + %%i
    set /a DPORT=BASE_DPORT + %%i
    set /a HPORT=BASE_HPORT + %%i

    if not exist !FOLDER!\.cfg (
        (
        echo {
        echo     "folder-path": "!FOLDER!",
        echo     "node-id": %%i,
        echo     "control-plane-port": !CPORT!,
        echo     "data-plane-port": !DPORT!,
        echo     "enable-logging": true,
        echo     "bootstrap-server-addr": "%BOOTSTRAP_ADDR%:45999",
        echo     "name-server-addr": "%NAMESERVER_ADDR%:45998",
        echo     "db-name": "chat-db.sql",
        echo     "http-server-port": "!HPORT!",
        echo     "template-directory": "templates",
        echo     "read-timeout": 10,
        echo     "write-timeout": 10,
        echo     "secret-key": "generic-robust-password-wink-wink"
        echo }
        ) > !FOLDER!\.cfg
    )
)

echo Setup completato.
pause