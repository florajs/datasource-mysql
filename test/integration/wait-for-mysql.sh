#!/bin/sh

set -e

host="$1"
shift
cmd="$@"

until node test/integration/wait-for-mysql.js; do
  >&2 echo "MySQL is unavailable - sleeping"
  sleep 1
done

>&2 echo "MySQL is up - executing command"
exec $cmd
