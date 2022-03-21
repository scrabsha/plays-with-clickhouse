use std::{collections::HashMap, error::Error};

use clickhouse::{Client, Row};
use serde::ser::{Serialize, SerializeMap, Serializer};

// Warning: this example will panic at run-time and contains incorrect
// hard-coded data. It is here as a proof that we can't use clickhouse for
// dynamically typed data rather than actual working code.

const DB_NAME: &str = "2-dynamic-data";
const TABLE_NAME: &str = "some";

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_database(DB_NAME);

    let mut insert = client.insert(TABLE_NAME)?;

    insert
        .write(&dynamic_struct! {
            name: "Ada Lovelace",
            age: 19,
        })
        .await?;

    insert
        .write(&dynamic_struct! {
            name: "Grace Hopper",
            age: 30,
        })
        .await?;

    insert.end().await?;

    Ok(())
}

struct DynamicStruct {
    data: HashMap<String, Value>,
}

impl Row for DynamicStruct {
    // Here's the problem: we have no clue of what the columns will be, since
    // they we will know them at run time only.
    const COLUMN_NAMES: &'static [&'static str] = &["we", "don't", "actually", "know"];
}

impl Serialize for DynamicStruct {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut inner = s.serialize_map(Some(self.data.len()))?;

        // Here's where fun happens.
        //
        // My first thought was that this would instant-panic because the key
        // does not match the corresponding COLUMN_NAME. Instead, it panics with
        // the following message:
        //
        // thread 'main' panicked at 'not yet implemented', /home/.../.cargo/registry/src/github.com-1ecc6299db9ec823/clickhouse-0.10.0/src/rowbinary/ser.rs:168:9
        // note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
        //
        // (a permalink to the corresponding code can be found at the end of
        // this comment).
        //
        // This makes me think that the `clickhouse` crates aims to work with
        // concrete types only, which is not our use case here.
        //
        // https://docs.rs/clickhouse/0.10.0/src/clickhouse/rowbinary/ser.rs.html#168
        self.data
            .iter()
            .try_for_each(|(k, v)| inner.serialize_entry(k, v))?;

        inner.end()
    }
}

enum Value {
    String(String),
    Integer(u32),
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::String(str) => s.serialize_str(str),
            Value::Integer(int) => s.serialize_i32(*int as _),
        }
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value {
        Value::String(s.to_string())
    }
}

impl From<u32> for Value {
    fn from(i: u32) -> Self {
        Value::Integer(i)
    }
}
