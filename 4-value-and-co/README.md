# Dynamic data

In this experiment, we insert tremor values in a clickhouse table. We know
all the columns and their types before actually getting the data.

## Setup

The following command must be run as root:

```
$ clickhouse-client --query "create database if not exists value_and_co"
```

## Example

```
$ cargo run
$ clickhouse-client --query "select * from value_and_co.people"
Grace	22
Alan	22
```