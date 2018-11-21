#!/bin/sh

set -e

HOST="$1"
PORT="$2"
DB="$3"
RETRIES=0
CONNECTED=true

until node test/integration/tool/wait-for-mysql.js $HOST $PORT $DB; do
  if [ $RETRIES -gt 50 ]
  then
    echo "Tried multiple times - giving up"
    CONNECTED=false
    break
  fi

  >&2 echo "MySQL is unavailable - sleeping"
  sleep 1
  RETRIES=$((RETRIES+1))
done

if [ $CONNECTED = true ]
then
  >&2 echo "Connected to MySQL ($HOST:$PORT) - running tests"
fi
