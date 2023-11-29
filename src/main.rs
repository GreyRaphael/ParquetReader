slint::include_modules!();

use chrono::{NaiveDateTime, NaiveTime};
use duckdb::{types::TimeUnit, types::Value, Connection, Result};
use native_dialog::FileDialog;
use slint::{StandardListViewItem, TableColumn, VecModel};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug)]
struct Schema {
    column_names: Vec<String>,
    column_types: Vec<String>,
}

fn read_duck_schema(filename: &str) -> Result<Schema> {
    let type_dict = HashMap::from([
        ("BOOLEAN", "bool"),
        ("TINYINT", "i8"),
        ("SMALLINT", "i16"),
        ("INTEGER", "i32"),
        ("BIGINT", "i64"),
        ("HUGEINT", "i128"),
        ("UTINYINT", "u8"),
        ("USMALLINT", "u16"),
        ("UINTEGER", "u32"),
        ("UBIGINT", "u64"),
        ("REAL", "f32"),
        ("DOUBLE", "f64"),
        ("DECIMAL", "decimal"),
        ("DATE", "i32"),
        ("TIME", "time"),
        ("TIMESTAMP", "datetime"),
        ("VARCHAR", "utf8"),
        ("BLOB", "Vec<u8>"),
    ]);

    let conn = Connection::open_in_memory()?;
    let schema_sql = std::format!("DESCRIBE SELECT * FROM read_parquet({})", filename);
    let mut stmt = conn.prepare(&schema_sql)?;
    let mut rows = stmt.query([])?;

    let mut schema = Schema {
        column_names: Vec::new(),
        column_types: Vec::new(),
    };
    while let Some(row) = rows.next()? {
        schema.column_names.push(row.get::<_, String>(0)?);
        schema.column_types.push(row.get::<_, String>(1)?);
    }

    schema.column_types = schema
        .column_types
        .into_iter()
        .map(|item| {
            type_dict
                .get(item.as_str())
                .unwrap_or(&item.as_str())
                .to_string()
        })
        .collect();

    Ok(schema)
}

fn read_duck_data(filename: &str, col_num: usize) -> Result<Vec<Vec<Value>>> {
    let conn = Connection::open_in_memory()?;
    let data_sql = std::format!("SELECT * FROM read_parquet({}) LIMIT 10", filename);
    let mut stmt = conn.prepare(&data_sql)?;
    let mut rows = stmt.query([])?;

    let mut table = Vec::new();
    while let Some(row) = rows.next()? {
        let row_data: Vec<Value> = (0..col_num).map(|i| row.get(i).unwrap()).collect();
        table.push(row_data);
    }

    Ok(table)
}

fn read_sqlite_table_names(filename: &str) -> Result<Vec<String>> {
    let conn = Connection::open_in_memory()?;
    // load sqlite file
    let pre_sql = std::format!("CALL sqlite_attach({});", filename);
    conn.execute(&pre_sql, [])?;
    // show all tables in sqlite
    let mut stmt = conn.prepare("PRAGMA show_tables;")?;

    let table_names = stmt
        .query_map([], |row| Ok(row.get::<_, String>(0)?))?
        .map(|row| row.unwrap())
        .collect();
    Ok(table_names)
}

fn read_sqlite_schema(filename: &str, table_name: &str) -> Result<Schema> {
    let type_dict = HashMap::from([
        ("BOOLEAN", "bool"),
        ("TINYINT", "i8"),
        ("SMALLINT", "i16"),
        ("INTEGER", "i32"),
        ("BIGINT", "i64"),
        ("HUGEINT", "i128"),
        ("UTINYINT", "u8"),
        ("USMALLINT", "u16"),
        ("UINTEGER", "u32"),
        ("UBIGINT", "u64"),
        ("REAL", "f32"),
        ("DOUBLE", "f64"),
        ("DECIMAL", "decimal"),
        ("DATE", "i32"),
        ("TIME", "time"),
        ("TIMESTAMP", "datetime"),
        ("VARCHAR", "utf8"),
        ("BLOB", "Vec<u8>"),
    ]);

    let conn = Connection::open_in_memory()?;
    let sql = std::format!(
        "DESCRIBE SELECT * FROM sqlite_scan({}, '{}');",
        filename,
        table_name
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;

    let mut schema = Schema {
        column_names: Vec::new(),
        column_types: Vec::new(),
    };
    while let Some(row) = rows.next()? {
        schema.column_names.push(row.get::<_, String>(0)?);
        schema.column_types.push(row.get::<_, String>(1)?);
    }

    schema.column_types = schema
        .column_types
        .into_iter()
        .map(|item| {
            type_dict
                .get(item.as_str())
                .unwrap_or(&item.as_str())
                .to_string()
        })
        .collect();

    Ok(schema)
}

fn test_table() {
    let path = FileDialog::new()
        .set_location("~")
        .add_filter("parquet file", &["parquet"])
        .show_open_single_file()
        .unwrap();

    let path = match path {
        Some(path) => path,
        None => return,
    };

    let filename = format!("{:#?}", path);
    print!("filename: {}\n", filename);
    let schema = read_duck_schema(&filename).unwrap();
    println!("{:?}", schema);
    let table = read_duck_data(&filename, schema.column_names.len()).unwrap();
    println!("{:?}", table);
}

fn button_pressed_handler(recipe_weak: slint::Weak<Example>) -> impl Fn() {
    move || {
        // open dialog
        let path = FileDialog::new()
            .set_location("~")
            .add_filter("parquet file", &["parquet"])
            .add_filter("all files", &["*"])
            .show_open_single_file()
            .unwrap();

        let path = match path {
            Some(path) => path,
            None => return,
        };

        // read file
        let filename = format!("{:#?}", path);
        let schema = read_duck_schema(&filename).unwrap();
        println!("{:?}", schema);
        let col_num = schema.column_names.len();
        let table = read_duck_data(&filename, col_num).unwrap();
        println!("{:?}", table);

        // init handler
        let recipe = recipe_weak.upgrade().unwrap();

        // fill data
        let datas = Rc::new(VecModel::default());
        for row in table {
            let items = Rc::new(VecModel::default());
            for cell in row {
                let item = create_list_view_item(&cell);
                items.push(item);
            }
            datas.push(items.into());
        }
        recipe.set_datas(datas.into());

        let headers = Rc::new(VecModel::default());
        for i in 0..col_num {
            let mut header = TableColumn::default();
            header.title = slint::format!("{}({})", schema.column_names[i], schema.column_types[i]);
            header.min_width = 80.0;
            headers.push(header);
        }
        recipe.set_headers(headers.into());
    }
}

fn create_list_view_item(cell: &Value) -> StandardListViewItem {
    let formatted_value = match cell {
        Value::Boolean(v) => v.to_string(),
        Value::TinyInt(v) => v.to_string(),
        Value::SmallInt(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::HugeInt(v) => v.to_string(),
        Value::UTinyInt(v) => v.to_string(),
        Value::USmallInt(v) => v.to_string(),
        Value::UInt(v) => v.to_string(),
        Value::UBigInt(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Double(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),

        Value::Date32(v) => NaiveDateTime::from_timestamp_opt((v * 24 * 3600) as i64, 0)
            .unwrap()
            .date()
            .to_string(),
        Value::Time64(TimeUnit::Nanosecond, v) => NaiveTime::from_num_seconds_from_midnight_opt(
            (v / 1_000_000_000) as u32,
            (v % 1_000_000_000) as u32,
        )
        .unwrap()
        .format("%H:%M:%S%.9f")
        .to_string(),
        Value::Time64(TimeUnit::Microsecond, v) => NaiveTime::from_num_seconds_from_midnight_opt(
            (v / 1_000_000) as u32,
            (v * 1_000 % 1_000_000_000) as u32,
        )
        .unwrap()
        .format("%H:%M:%S%.6f")
        .to_string(),
        Value::Time64(TimeUnit::Millisecond, v) => NaiveTime::from_num_seconds_from_midnight_opt(
            (v / 1_000) as u32,
            (v * 1_000_000 % 1_000_000_000) as u32,
        )
        .unwrap()
        .format("%H:%M:%S%.3f")
        .to_string(),
        Value::Time64(TimeUnit::Second, v) => {
            NaiveTime::from_num_seconds_from_midnight_opt(*v as u32, 0)
                .unwrap()
                .format("%H:%M:%S%")
                .to_string()
        }
        Value::Timestamp(TimeUnit::Nanosecond, v) => {
            NaiveDateTime::from_timestamp_opt(v / 1_000_000_000, (v % 1_000_000_000) as u32)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S%.9f")
                .to_string()
        }
        Value::Timestamp(TimeUnit::Microsecond, v) => {
            NaiveDateTime::from_timestamp_opt(v / 1_000_000, (v * 1_000 % 1_000_000_000) as u32)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string()
        }
        Value::Timestamp(TimeUnit::Millisecond, v) => {
            NaiveDateTime::from_timestamp_opt(v / 1_000, (v * 1_000_000 % 1_000_000_000) as u32)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
        }
        Value::Timestamp(TimeUnit::Second, v) => NaiveDateTime::from_timestamp_opt(*v, 0)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
        Value::Text(v) => v.to_string(),
        Value::Blob(v) => std::format!("{:?}", v),
        _ => format!("{:?}", cell),
    };

    StandardListViewItem::from(slint::format!("{}", formatted_value))
}

fn main() {
    // let recipe = Example::new().unwrap();
    // let recipe_weak = recipe.as_weak();
    // recipe.on_button_pressed(button_pressed_handler(recipe_weak));
    // recipe.run().unwrap();
    // update_table();

    let v = read_sqlite_table_names("\"D:\\Dev\\sqlite-gui\\bookstore.sqlite\"");
    println!("{:?}", v);
    let v = read_sqlite_schema("\"D:\\Dev\\sqlite-gui\\bookstore.sqlite\"", "books");
    println!("{:?}", v);
}
