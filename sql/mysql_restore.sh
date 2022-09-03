#!/bin/bash


if [ `hostname` == "a01-28" ]; then
  ssh i2 "bash -c '~/webapp/sql/mysql_restore.sh'" &
  ssh i3 "bash -c '~/webapp/sql/mysql_restore.sh'" &
  ssh i4 "bash -c '~/webapp/sql/mysql_restore.sh'" &
  ssh i5 "bash -c '~/webapp/sql/mysql_restore.sh'" &
  wait
fi


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
