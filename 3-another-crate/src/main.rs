use std::{collections::HashMap, error::Error};

use clickhouse_rs::{Block, Pool};

// This macro aims to make it simpler to create dynamic structs.
//
// It is placed at the top because Rust macros *must* be declared before their
// usage.
macro_rules! dynamic_struct {
    ($( $key:ident: $val:expr ),* $(,)? ) => {
        DynamicStruct {
            data: {
                let mut tmp = HashMap::new();
                $( tmp.insert(stringify!($key).to_string(), $val.into()); )*
                tmp
            }
        }
    };
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS people (
            name String,
            age UInt8
        ) Engine=Memory";

    let first = dynamic_struct! {
        name: "Grace",
        age: 22,
    };
    let second = dynamic_struct! {
        name: "Alan",
        age: 22,
    };
    let people = [first, second];

    let block = block_from_dynamic(people.as_slice());

    let pool = Pool::new("tcp://localhost:9000/another_crate");

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;
    client.insert("people", block).await?;
    let block = client.query("SELECT * FROM people").fetch_all().await?;

    for row in block.rows() {
        let name: &str = row.get("name")?;
        let age: u8 = row.get("age")?;
        println!("Found person {}: {}", name, age);
    }
    Ok(())
}

fn block_from_dynamic(rows: &[DynamicStruct]) -> Block {
    // Under normal circumstances, we would not trigger a batch insert. As this
    // is testing code, we will instead panic.
    let first = rows.first().expect("Can't get columns for empty batch");

    let columns = columns_of(first);

    create_block_from_columns(columns, rows)
}

fn columns_of(struct_: &DynamicStruct) -> Vec<(&str, Ty)> {
    struct_
        .data
        .iter()
        .map(|(k, v)| (k.as_str(), v.kind()))
        .collect()
}

fn create_block_from_columns(columns: Vec<(&str, Ty)>, rows: &[DynamicStruct]) -> Block {
    columns
        .into_iter()
        .fold(Block::new(), |block, (col, ty)| match ty {
            Ty::Integer => collect_integers(block, col, rows),
            Ty::String => collect_strings(block, col, rows),
        })
}

fn collect_integers(block: Block, col: &str, rows: &[DynamicStruct]) -> Block {
    let values = collect_data(col, rows, Value::as_integer);
    block.column(col, values)
}

fn collect_strings(block: Block, col: &str, rows: &[DynamicStruct]) -> Block {
    let values = collect_data(col, rows, Value::as_string);
    block.column(col, values)
}

fn collect_data<'a, T: 'a, F: Fn(&'a Value) -> T>(
    col: &str,
    rows: &'a [DynamicStruct],
    f: F,
) -> Vec<T> {
    rows.iter()
        .map(|row| f(row.data.get(col).unwrap()))
        .collect()
}

struct DynamicStruct {
    data: HashMap<String, Value>,
}

enum Value {
    String(String),
    Integer(u8),
}

impl Value {
    fn as_string(&self) -> &str {
        if let Self::String(v) = self {
            v
        } else {
            panic!();
        }
    }

    fn as_integer(&self) -> u8 {
        if let Self::Integer(v) = self {
            *v
        } else {
            panic!()
        }
    }

    fn kind(&self) -> Ty {
        match self {
            Value::String(_) => Ty::String,
            Value::Integer(_) => Ty::Integer,
        }
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value {
        Value::String(s.to_string())
    }
}

impl From<u8> for Value {
    fn from(i: u8) -> Self {
        Value::Integer(i)
    }
}

enum Ty {
    Integer,
    String,
}
