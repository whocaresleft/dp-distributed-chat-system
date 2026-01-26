#!/bin/bash

echo "Creating BOOTSTRAP env..."
mkdir -p B0OTSTRAP
touch B0OTSTRAP/bootstrap.log
touch B0OTSTRAP/bootstrap.cfg

echo "Creating Nodes env..."
BASE_CPORT=46000
BASE_DPORT=50000
BASE_HPORT=8080
for i in {1..4}
do
    FOLDER="NODE_$i"
    mkdir -p "$FOLDER"

    CPORT=$((BASE_CPORT + i))
    DPORT=$((BASE_DPORT + i))
    HPORT=$((BASE_HPORT + i))
    if [ ! -f "$FOLDER/.cfg" ]; then
cat <<EOF > "$FOLDER/.cfg"
{
    "folder-path": "$FOLDER",
    "node-id": $i,
    "control-plane-port": $CP_PORT,
    "data-plane-port": $DP_PORT,
    "enable-logging": true,
    "bootstrap-server-addr": "127.0.0.1:45999",
    "name-server-addr": "127.0.0.1:45998",
    "db-name": "chat-db.sql",
    "http-server-port": "$HTTP_PORT",
    "template-directory": "templates",
    "read-timeout": 10,
    "write-timeout": 10,
    "secret-key": "generic-robust-password-wink-wink"
}
EOF
    fi
done

echo "Setup complete. You can manually modify ./NODE_<id>/.cfg to your likings. 6 Nodes were setup, from NODE_1 to NODE_6"