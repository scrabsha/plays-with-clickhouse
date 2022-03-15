# Hello, world!

In this example we try to insert hardcoded data to the database using a very
small batch.

## Setup

First, we need to start the clickhouse server:

```bash
$ sudo clickhouse start
```

We then need to create the database for the example:

```bash
$ clickhouse-client --query "create database if not exists 1_hello_world"
```

We can now create a super simple table:

```bash
$ clickhouse-client --query "create table 1_hello_world.some (`name` String, `age` UInt8) ENGINE = MergeTree() order by age"