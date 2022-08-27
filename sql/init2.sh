#!/bin/sh
set -ex
cd `dirname $0`

ISUCON_DB_HOST=${ISUCON_DB2_HOST:-127.0.0.1}
ISUCON_DB_PORT=${ISUCON_DB2_PORT:-3306}
ISUCON_DB_USER=${ISUCON_DB2_USER:-isucon}
ISUCON_DB_PASSWORD=${ISUCON_DB2_PASSWORD:-isucon}
ISUCON_DB_NAME=${ISUCON_DB2_NAME:-isucon}

mysql -u"$ISUCON_DB_USER" \
		-p"$ISUCON_DB_PASSWORD" \
		--host "$ISUCON_DB_HOST" \
		--port "$ISUCON_DB_PORT" \
		"$ISUCON_DB_NAME" < 3_schema_exclude_user_presents.sql

mysql -u"$ISUCON_DB_USER" \
		-p"$ISUCON_DB_PASSWORD" \
		--host "$ISUCON_DB_HOST" \
		--port "$ISUCON_DB_PORT" \
		"$ISUCON_DB_NAME" < 4_alldata_exclude_user_presents.sql

echo "delete from user_presents_exists where id > 100000000000" | mysql -u"$ISUCON_DB_USER" \
		-p"$ISUCON_DB_PASSWORD" \
		--host "$ISUCON_DB_HOST" \
		--port "$ISUCON_DB_PORT" \
		"$ISUCON_DB_NAME"
echo "delete from user_presents_deleted where id > 100000000000" | mysql -u"$ISUCON_DB_USER" \
		-p"$ISUCON_DB_PASSWORD" \
		--host "$ISUCON_DB_HOST" \
		--port "$ISUCON_DB_PORT" \
		"$ISUCON_DB_NAME"

DIR=`mysql -u"$ISUCON_DB_USER" -p"$ISUCON_DB_PASSWORD" -h "$ISUCON_DB_HOST" -Ns -e "show variables like 'secure_file_priv'" | cut -f2`
SECURE_DIR=${DIR:-/var/lib/mysql-files/}

scp 5_user_presents_not_receive_data.tsv i5:${SECURE_DIR}

#ssh i5 sh -c "sudo cp 5_user_presents_not_receive_data.tsv ${SECURE_DIR}"

echo "LOAD DATA INFILE '${SECURE_DIR}5_user_presents_not_receive_data.tsv' REPLACE INTO TABLE user_presents_tmp FIELDS ESCAPED BY '|' IGNORE 1 LINES ;" | mysql -u"$ISUCON_DB_USER" \
        -p"$ISUCON_DB_PASSWORD" \
        --host "$ISUCON_DB_HOST" \
        --port "$ISUCON_DB_PORT" \
        "$ISUCON_DB_NAME" 

echo "LOAD DATA INFILE '${SECURE_DIR}5_user_presents_not_receive_data.tsv' REPLACE INTO TABLE user_presents_exists FIELDS ESCAPED BY '|' IGNORE 1 LINES ;" | mysql -u"$ISUCON_DB_USER" \
        -p"$ISUCON_DB_PASSWORD" \
        --host "$ISUCON_DB_HOST" \
        --port "$ISUCON_DB_PORT" \
        "$ISUCON_DB_NAME"

echo "DELETE FROM user_presents_deleted WHERE id IN (SELECT id FROM user_presents_tmp)" | mysql -u"$ISUCON_DB_USER" \
        -p"$ISUCON_DB_PASSWORD" \
        --host "$ISUCON_DB_HOST" \
        --port "$ISUCON_DB_PORT" \
        "$ISUCON_DB_NAME"
