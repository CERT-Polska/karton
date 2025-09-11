#!/usr/bin/env bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
SECRET_KEY=$(od -vN 18 -An -tx1 /dev/urandom | tr -d " \n")

echo "KARTON_GATEWAY-SERVER_SECRET_KEY=$SECRET_KEY" > "$parent_path/gateway-secret.env"
echo "$parent_path/gateway-secret.env generated."
