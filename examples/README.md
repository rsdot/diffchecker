# diffchecker

## install 3rd party tools (MacOSX)

```bash
brew install mycli
brew install csvkit
```

## set env variables

```bash
export DFC_SRC_USERNAME=xxxx
export DFC_SRC_PASSWORD='xxxx'
export DFC_SRC_HOST=127.0.0.1
export DFC_SRC_PORT=3306
export DFC_SRC_DBNAME=employees
export DFC_TGT_USERNAME=xxxx
export DFC_TGT_PASSWORD='xxxx'
export DFC_TGT_HOST=127.0.0.1
export DFC_TGT_PORT=3306
export DFC_TGT_DBNAME=employees2
```

## load sample DB data

```bash
mysqld_safe --datadir=/usr/local/var/mysql &

curl -L https://github.com/datacharmer/test_db/releases/download/v1.0.7/test_db-1.0.7.tar.gz -o /tmp/test_db-1.0.7.tar.gz
cd /tmp/
tar xzf test_db-1.0.7.tar.gz
cd /tmp/test_db
MYSQL_PWD=$DFC_SRC_PASSWORD mysql -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_SRC_USERNAME -e "drop database if exists employees; create database employees"
MYSQL_PWD=$DFC_SRC_PASSWORD mysql -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_SRC_USERNAME < employees.sql
MYSQL_PWD=$DFC_SRC_PASSWORD mysql -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_SRC_USERNAME -t < test_employees_md5.sql
MYSQL_PWD=$DFC_SRC_PASSWORD mysqldump -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_SRC_USERNAME employees > employees_dump.sql
MYSQL_PWD=$DFC_TGT_PASSWORD mysql -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -e "drop database if exists employees2; create database employees2"
MYSQL_PWD=$DFC_TGT_PASSWORD mysql -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -D employees2 < employees_dump.sql
```

## test 1 PK field table

```bash
export table=employees
export chunksize=10000
```

### diff

```bash
bin/diffchecker diff -c $chunksize --table $table -o /tmp/dfclog.$table.$chunksize.json

## lower boundary
bin/diffchecker diff -c $chunksize --table $table -l 479950 -o /tmp/dfclog.$table.$chunksize.json

## lower boundary, ignore 2 fields
bin/diffchecker diff -c $chunksize --table $table -l 479950 -I first_name,last_name -o /tmp/dfclog.$table.$chunksize.json
## ┌                                                                              ┐
## │ update target table                                                          │
## └                                                                              ┘
mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -e "update employees set first_name = reverse(first_name) where emp_no between 499940 and 499950;"
## lower boundary, ignore 2 fields, no difference
bin/diffchecker diff -c $chunksize --table $table -l 479950 -I first_name,last_name -o /tmp/dfclog.$table.$chunksize.json
## lower boundary, with difference of update
bin/diffchecker diff -c $chunksize --table $table -l 479950 -o /tmp/dfclog.$table.$chunksize.json

## ┌                                                                              ┐
## │ delete target table                                                          │
## └                                                                              ┘
mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -e "delete from employees where emp_no between 489940 and 489960;"

## ┌                                                                              ┐
## │ delete source table                                                          │
## └                                                                              ┘
mycli -h $DFC_SRC_HOST -P $DFC_SRC_PORT -u $DFC_SRC_USERNAME -p $DFC_SRC_PASSWORD -D $DFC_SRC_DBNAME -e "delete from employees where emp_no IN (489900, 499900, 499990);"

## lower boundary, show difference for delete update and insert
bin/diffchecker diff -c $chunksize --table $table -l 479950 -o /tmp/dfclog.$table.$chunksize.json
```

### query

```bash
## sql for delete
bin/diffchecker query -f /tmp/dfclog.$table.$chunksize.rowlevel.json -d
## sql for insert
bin/diffchecker query -f /tmp/dfclog.$table.$chunksize.rowlevel.json -i
## sql for update
bin/diffchecker query -f /tmp/dfclog.$table.$chunksize.rowlevel.json -u
```

### sync

```bash
## run --target section of sql for delete

## run --source section of sql for insert
examples/dump_and_load.sh $table insert
## run --target section of sql for insert

## run --source section of sql for update
examples/dump_and_load.sh $table update
## run --target section of sql for update

## lower boundary, no difference
bin/diffchecker diff -c $chunksize --table $table -l 479950 -o /tmp/dfclog.$table.$chunksize.json
```

## test 3 PK fields table

```bash
export table=titles
export chunksize=10000
```

### diff

```bash
# all 3 PK fields, default PK sequence, take long time to complete
bin/diffchecker diff -c $chunksize --table $table -o /tmp/dfclog.$table.$chunksize.json

# only 2 PK fields, customized PK sequence
bin/diffchecker diff -c $chunksize --table $table -S 2,3 -o /tmp/dfclog.$table.$chunksize.json
# only 2 PK fields, lower boundary with customized PK sequence
bin/diffchecker diff -c $chunksize --table $table -l "Technique Leader","1969-12-31T19:00:00-05:00" -S 2,3 -o /tmp/dfclog.$table.$chunksize.json
# only 2 PK fields, upper boundary with customized PK sequence
bin/diffchecker diff -c $chunksize --table $table -u "Engineer","1987-03-10T00:00:00Z" -S 2,3 -o /tmp/dfclog.$table.$chunksize.json
# only 2 PK fields, lower and upper boundary with customized PK sequence
bin/diffchecker diff -c $chunksize --table $table -l "Staff","1997-06-28" -u "Technique Leader","1986-07-12" -S 2,3 -o /tmp/dfclog.$table.$chunksize.json

# only 2 PK fields, lower and upper boundary with customized PK sequence, ignore 1 field
bin/diffchecker diff -c $chunksize --table $table -l "Staff","1997-06-28" -u "Technique Leader","1986-07-12" -S 2,3 -I to_date -o /tmp/dfclog.$table.$chunksize.json
## ┌                                                                              ┐
## │ update target table                                                          │
## └                                                                              ┘
mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -e "update titles set to_date = now() where title IN ('Staff') and from_date between '1998-11-21' and '1998-11-25';"
# only 2 PK fields, lower and upper boundary with customized PK sequence, ignore 1 field
bin/diffchecker diff -c $chunksize --table $table -l "Staff","1997-06-28" -u "Technique Leader","1986-07-12" -S 2,3 -I to_date -o /tmp/dfclog.$table.$chunksize.json
# only 2 PK fields, lower and upper boundary with customized PK sequence, with difference of update
bin/diffchecker diff -c $chunksize --table $table -l "Staff","1997-06-28" -u "Technique Leader","1986-07-12" -S 2,3 -o /tmp/dfclog.$table.$chunksize.json

## ┌                                                                              ┐
## │ delete target table                                                          │
## └                                                                              ┘
mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -e "delete from titles where title IN ('Staff') and from_date between '1998-01-05' and '1998-01-10';"
mycli -h $DFC_TGT_HOST -P $DFC_TGT_PORT -u $DFC_TGT_USERNAME -p $DFC_TGT_PASSWORD -D $DFC_TGT_DBNAME -e "delete from titles where title IN ('Staff') and from_date between '2000-01-25' and '2000-02-01';"

## ┌                                                                              ┐
## │ delete source table                                                          │
## └                                                                              ┘
mycli -h $DFC_SRC_HOST -P $DFC_SRC_PORT -u $DFC_SRC_USERNAME -p $DFC_SRC_PASSWORD -D $DFC_SRC_DBNAME -e "delete from titles where title IN ('Technique Leader') and from_date between '1986-05-01' and '1986-05-05';"

# only 2 PK fields, lower and upper boundary with customized PK sequence, show difference for delete update and insert
bin/diffchecker diff -c $chunksize --table $table -l "Staff","1997-06-28" -u "Technique Leader","1986-07-12" -S 2,3 -o /tmp/dfclog.$table.$chunksize.json
```

### query

```bash
## sql for delete
bin/diffchecker query -f /tmp/dfclog.$table.$chunksize.rowlevel.json -d
## sql for insert
bin/diffchecker query -f /tmp/dfclog.$table.$chunksize.rowlevel.json -i
## sql for update
bin/diffchecker query -f /tmp/dfclog.$table.$chunksize.rowlevel.json -u
```

### sync

```bash
## run --target section of sql for delete

## run --source section of sql for insert
examples/dump_and_load.sh $table insert
## run --target section of sql for insert

## run --source section of sql for update
examples/dump_and_load.sh $table update
## run --target section of sql for update

## lower boundary, no difference
bin/diffchecker diff -c $chunksize --table $table -l "Staff","1997-06-28" -u "Technique Leader","1986-07-12" -S 2,3 -o /tmp/dfclog.$table.$chunksize.json
```

