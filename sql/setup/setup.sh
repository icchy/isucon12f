#!/bin/bash
set -x
cd `dirname $0`

if [ `hostname` == "a01-28" ]; then
    ./setup_admin.sh
else
    ./setup_user.sh
fi
