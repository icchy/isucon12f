#!/bin/bash

set -eu

mysql -u isucon -pisucon isucon < ./split_present_table.sql
