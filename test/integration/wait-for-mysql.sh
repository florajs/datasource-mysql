#!/bin/sh

set -e

host="$1"
port="$2"

until node test/integration/wait-for-mysql.js $host $port; do
  >&2 echo "MySQL is unavailable - sleeping"
  sleep 1
done

>&2 echo "MySQL is up - running tests"
