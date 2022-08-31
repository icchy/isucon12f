#!/bin/bash

if [ ! -f ~/mysqldata.tar.gz ]; then
    echo "mysql dump not found"
    exit 1
fi

sudo systemctl stop mysql
cd /var/lib
sudo rm -rf mysql
sudo tar zxvf ~/mysqldata.tar.gz
sudo systemctl start mysql

echo "set global slow_query_log_file = '/var/log/mysql/mysql-slow.log'; set global long_query_time = 0; set global slow_query_log = ON;" | sudo mysql