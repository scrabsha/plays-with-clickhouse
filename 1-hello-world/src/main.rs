use std::error::Error;

use clickhouse::{Client, Row};
use serde::Serialize;

const DB_NAME: &str = "1_hello_world";
const TABLE_NAME: &str = "some";

#[derive(Row, Serialize)]
struct Data<'a> {
    name: &'a str,
    // Note: this is not future-proof.
    age: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_database(DB_NAME);

    let mut insert = client.insert(TABLE_NAME)?;

    insert
        .write(&Data {
            name: "Ada Lovelace",
            age: 21,
        })
        .await?;

    insert
        .write(&Data {
            name: "Grace Hopper",
            age: 30,
        })
        .await?;

    insert.end().await?;

    Ok(())
}
