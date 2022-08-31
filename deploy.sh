#!/bin/bash

set -eu

APP_HOST=i1

(cd go && GOOS=linux GOARCH=amd64 go build -o ../webapp)
scp webapp ${APP_HOST}:/tmp/webapp
rsync -vr sql/* $APP_HOST:webapp/sql/

# DB_HOSTS="i2 i3 i4 i5"
# for h in $DB_HOSTS
# do
#   rsync -v sql/* $h:webapp/sql/
# done

ssh ${APP_HOST} sh -c "set -eu
mv /tmp/webapp /home/isucon/webapp/go/isuconquest
make prebench
"
