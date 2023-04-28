# diffchecker

A CLI tool to compare database table data

_Currently support **MySQL** compatible DB tables ONLY_

## Why?


- 💔 **Frustration**:

  I wasn't able to find an open source product that is able to compare subset of two huge MySQL compatible tables quickly, so that's how **diffchecker** was created</ol>

- 🌟 **Featured**:
  1. Diff two **MySQL** compatible database tables data using CRC32 Hash.
  1. Diff **subset of table data** with user defined Lower Boundary and Upper Boundary based on PK fields.
  1. Source and Target table name could be different, but with identical schema.
  1. **Ignoring table fields** in data compare.
  1. Applying **user defined filter** for where clause in data compare.
  1. **Customized PK field sequence** for chunk query for much better performance.
  1. Generating **sql CRUD code** for data sync. working with tool [mycli](https://github.com/dbcli/mycli), [csvkit](https://github.com/wireservice/csvkit).

## Build

```bash
make clean
make build
```

## Usage

```bash
bin/diffchecker -h
bin/diffchecker diff -h
bin/diffchecker query -h
```

## usage examples

[examples/README.md](examples/README.md)

