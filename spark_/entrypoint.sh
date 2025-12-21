#!/bin/bash
set -e

if [ "$1" = "driver" ]; then
  shift
  exec /opt/spark/bin/spark-submit "$@"
else
  exec "$@"
fi

