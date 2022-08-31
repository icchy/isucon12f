#!/bin/bash

echo "purge binary logs before '2023-01-01 00:00:00';" | sudo mysql
sudo systemctl stop mysql
cd /var/lib/
sudo rm ~/mysqldata.tar.gz
sudo tar czvf ~/mysqldata.tar.gz ./mysql
sudo systemctl start mysql