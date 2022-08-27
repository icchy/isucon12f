#!/bin/bash

DEPLOY_HOST=i2

(cd go && GOOS=linux GOARCH=amd64 go build -o ../webapp)
ssh ${DEPLOY_HOST} sh -c "mkdir -p /tmp/webapp"
scp webapp ${DEPLOY_HOST}:/tmp/webapp
ssh ${DEPLOY_HOST} sh -c "set -eu
mv /tmp/webapp /home/isucon/webapp/go/isuconquest
make prebench"
