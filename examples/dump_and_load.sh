#!/usr/bin/env bash

table=$1
crudtype=$2

sql2csv --db "mysql://$DFC_SRC_USERNAME:$DFC_SRC_PASSWORD@$DFC_SRC_HOST:$DFC_SRC_PORT/$DFC_SRC_DBNAME" --query "select * from ${table}_diff_${crudtype}" > /tmp/${table}_diff_${crudtype}.csv

mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -te "drop table if exists ${table}_diff_${crudtype}"
MYSQL_PWD=$DFC_SRC_PASSWORD mysqldump --skip-add-drop-table -h $DFC_SRC_HOST -P $DFC_SRC_PORT -u $DFC_SRC_USERNAME -d $DFC_SRC_DBNAME ${table}_diff_${crudtype} | mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME
csvsql --db "mysql://$DFC_TGT_USERNAME:$DFC_TGT_PASSWORD@$DFC_TGT_HOST:$DFC_TGT_PORT/$DFC_TGT_DBNAME" --no-create --insert --chunk-size 1000 --tables ${table}_diff_${crudtype} /tmp/${table}_diff_${crudtype}.csv
mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -te "select count(1) from ${table}_diff_${crudtype}"

