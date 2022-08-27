#!/bin/bash

(cd go && go build -o ../webapp)
scp webapp i1:/tmp/webapp
ssh i1 sh -c "set -eu
mv /tmp/webapp /home/isucon/webapp/go/isuconquest
make prebench"
