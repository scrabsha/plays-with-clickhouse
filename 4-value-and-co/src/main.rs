use std::error::Error;

use clickhouse_rs::{Block, Pool};
use tremor_value::{json, prelude::ValueAccessTrait, Value};

// This experiment started as a translation of the previous one with tremore
// values, but we will supposed that we know the columns and their types
// *before* getting the data.

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Some initial setup.
    let ddl = r"
        CREATE TABLE IF NOT EXISTS people (
            name String,
            age UInt8
        ) Engine=Memory";

    // Let's suppose we read information about the columns and their types from
    // a troy file.
    let columns = [("name", Type::String), ("age", Type::UInt8)];

    // Let's suppose we got a bunch of values generated by the node DAG we need
    // to store in a database.
    let people = generate_value_to_insert();

    let block = block_from_values(&columns, people.as_slice());

    let pool = Pool::new("tcp://localhost:9000/value_and_co");

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;
    client.insert("people", block).await?;

    Ok(())
}

fn generate_value_to_insert() -> Vec<Value<'static>> {
    let first = json!({
        "name": "Grace",
        "age": 22u8,
    })
    .into();

    let second = json!({
        "name": "Alan",
        "age": 22u8,
    })
    .into();

    [first, second].to_vec()
}

fn block_from_values(columns: &[(&str, Type)], rows: &[Value]) -> Block {
    // We could transform rows: &[Value] to &[HashMap<&str, Value>] in order
    // to remove some runtime checks. That's ok for now.
    columns
        .iter()
        .fold(Block::new(), |block, (column_name, ty)| match ty {
            Type::String => append_string_column(block, column_name, rows),
            Type::UInt8 => append_int_column(block, column_name, rows),
        })
}

fn append_int_column(block: Block, column_name: &str, rows: &[Value]) -> Block {
    let mut buff = Vec::with_capacity(rows.len());
    for row in rows {
        // We have *a lot* of unwrap here. Under normal circumstances, we would
        // replace it with fancy error handling code. That's normal. Please
        // relax and enjoy your ride.
        let value = row
            .as_object()
            .unwrap()
            .get(column_name)
            .unwrap()
            .as_u8()
            .unwrap();

        buff.push(value);
    }

    block.add_column(column_name, buff)
}

fn append_string_column(block: Block, column_name: &str, rows: &[Value]) -> Block {
    let mut buff = Vec::with_capacity(rows.len());
    for row in rows {
        // We have *a lot* of unwrap here. Under normal circumstances, we would
        // replace it with fancy error handling code. That's normal. Please
        // relax and enjoy your ride.
        let value = row
            .as_object()
            .unwrap()
            .get(column_name)
            .unwrap()
            .as_str()
            .unwrap();

        buff.push(value);
    }

    let column = rows
        .iter()
        .map(|value| value_from_typed_attribute(value, column_name, Value::as_str))
        .collect::<Vec<_>>();

    block.add_column(column_name, column)
}

fn value_from_typed_attribute<'a, O, F>(value: &'a Value<'a>, attribute_name: &str, f: F) -> O
where
    F: Fn(&'a Value<'a>) -> Option<O>,
    O: 'a,
{
    // Under normal circumstances, we would replace these unwraps with fancy
    // error handling code. Let's keep things simple for now. Please relax and
    // enjoy your ride.

    let value = value.as_object().unwrap().get(attribute_name).unwrap();
    f(value).unwrap()
}

enum Type {
    UInt8,
    String,
}