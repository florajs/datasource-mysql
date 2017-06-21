#!/bin/sh

set -e

host="$1"
shift
cmd="$@"

until node test/wait-for-mysql.js; do
  >&2 echo "MySQL is unavailable - sleeping"
  sleep 1
done

>&2 echo "MySQL is up - running tests"
exec $cmd
