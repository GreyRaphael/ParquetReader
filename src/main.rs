#![windows_subsystem = "windows"]
slint::include_modules!();
use arrow::{
    array::{self, Array},
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{NaiveDateTime, NaiveTime};
use native_dialog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use slint::{ModelRc, StandardListViewItem, TableColumn, VecModel};
use std::rc::Rc;
use std::{fs::File, io::Error};

fn read_parquet(filename: &str, skip: usize, limit: usize) -> Result<RecordBatch, Error> {
    let file = File::open(filename).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder
        .with_offset(skip)
        .with_batch_size(limit)
        .build()
        .unwrap();

    let record_batch = reader.next().unwrap().unwrap();
    Ok(record_batch)
}

fn test_table() {
    if let Some(filename) = openfile_name() {
        println!("{}", filename);
        let _rb = read_parquet(&filename, 0, 3).unwrap();
        // prepare_data(&rb);
    }
}

fn openfile_name() -> Option<String> {
    let path = native_dialog::FileDialog::new()
        // .set_location("~")
        .reset_location()
        .add_filter("parquet file", &["parquet"])
        .show_open_single_file()
        .unwrap();

    match path {
        Some(path) => Some(path.to_string_lossy().into_owned()),
        None => None,
    }
}

fn generate_headers(rb: &RecordBatch) -> Rc<VecModel<TableColumn>> {
    let schema = rb.schema();
    let column_names: Vec<&String> = schema
        .all_fields()
        .into_iter()
        .map(|field| field.name())
        .collect();
    // println!("{:?}", column_names);
    let column_typenames: Vec<String> = schema
        .all_fields()
        .into_iter()
        .map(|field| std::format!("{:?}", field.data_type()))
        .collect();
    // println!("{:?}", column_typenames);

    let headers = Rc::new(VecModel::default());
    for i in 0..rb.num_columns() {
        let mut header = TableColumn::default();
        header.title = slint::format!("{}({})", column_names[i], column_typenames[i]);
        header.min_width = 80.0;
        // println!("{:?}", header);
        headers.push(header);
    }
    return headers;
}

fn generate_body(rb: &RecordBatch) -> Rc<VecModel<ModelRc<StandardListViewItem>>> {
    // read by column
    let mut table_strings: Vec<Vec<String>> = Vec::with_capacity(rb.num_columns());
    for col in rb.columns() {
        // println!("{:?}", col.data_type());
        let mut column_strings: Vec<String> = Vec::with_capacity(rb.num_rows());
        match col.data_type() {
            DataType::UInt8 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::UInt8Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::UInt16 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::UInt16Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::UInt32 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::UInt32Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::UInt64 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::UInt64Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Int8 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Int8Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Int16 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Int16Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Int32 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Int32Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Int64 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Int64Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Float64 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Float64Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Float32 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Float32Array>() {
                    for v in data_array.values() {
                        column_strings.push(v.to_string());
                    }
                }
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                if let Some(data_array) =
                    col.as_any().downcast_ref::<array::Time64NanosecondArray>()
                {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveTime::from_num_seconds_from_midnight_opt(
                                (*v / 1_000_000_000) as u32,
                                (*v % 1_000_000_000) as u32,
                            )
                            .unwrap()
                            .to_string(),
                        )
                    }
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                if let Some(data_array) =
                    col.as_any().downcast_ref::<array::Time64MicrosecondArray>()
                {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveTime::from_num_seconds_from_midnight_opt(
                                (*v / 1_000_000) as u32,
                                (*v * 1_000 % 1_000_000_000) as u32,
                            )
                            .unwrap()
                            .to_string(),
                        )
                    }
                }
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                if let Some(data_array) =
                    col.as_any().downcast_ref::<array::Time32MillisecondArray>()
                {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveTime::from_num_seconds_from_midnight_opt(
                                (*v / 1_000) as u32,
                                (*v * 1_000_000 % 1_000_000_000) as u32,
                            )
                            .unwrap()
                            .to_string(),
                        )
                    }
                }
            }
            DataType::Time32(TimeUnit::Second) => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Time32SecondArray>() {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveTime::from_num_seconds_from_midnight_opt(*v as u32, 0)
                                .unwrap()
                                .to_string(),
                        )
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                if let Some(data_array) = col
                    .as_any()
                    .downcast_ref::<array::TimestampNanosecondArray>()
                {
                    for v in data_array.values() {
                        let seconds: i64;
                        let nanos: u32;
                        if *v > 0 {
                            seconds = v / 1_000_000_000;
                            nanos = (v % 1_000_000_000) as u32;
                        } else {
                            seconds = v / 1_000_000_000 - 1;
                            nanos = (v % 1000000000 + 1000000000) as u32
                        }
                        column_strings.push(
                            NaiveDateTime::from_timestamp_opt(seconds, nanos)
                                .unwrap()
                                .to_string(),
                        );
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if let Some(data_array) = col
                    .as_any()
                    .downcast_ref::<array::TimestampMicrosecondArray>()
                {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveDateTime::from_timestamp_micros(*v)
                                .unwrap()
                                .to_string(),
                        )
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                if let Some(data_array) = col
                    .as_any()
                    .downcast_ref::<array::TimestampMillisecondArray>()
                {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveDateTime::from_timestamp_millis(*v)
                                .unwrap()
                                .to_string(),
                        )
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::TimestampSecondArray>()
                {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveDateTime::from_timestamp_opt(*v, 0)
                                .unwrap()
                                .to_string(),
                        )
                    }
                }
            }
            DataType::Date32 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::Date32Array>() {
                    for v in data_array.values() {
                        column_strings.push(
                            NaiveDateTime::from_timestamp_opt((v * 24 * 3600) as i64, 0)
                                .unwrap()
                                .date()
                                .to_string(),
                        )
                    }
                }
            }
            DataType::LargeUtf8 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::LargeStringArray>() {
                    for i in 0..data_array.len() {
                        column_strings.push(data_array.value(i).to_string());
                    }
                }
            }
            DataType::Utf8 => {
                if let Some(data_array) = col.as_any().downcast_ref::<array::StringArray>() {
                    for i in 0..data_array.len() {
                        column_strings.push(data_array.value(i).to_string());
                    }
                }
            }
            _ => {
                for _i in 0..col.len() {
                    column_strings.push("".to_string());
                }
            }
        }
        table_strings.push(column_strings.into());
    }

    // transpose data
    // println!("{:?}", table_strings);
    let datas = Rc::new(VecModel::default());
    for i in 0..rb.num_rows() {
        let items = Rc::new(VecModel::default());
        for j in 0..rb.num_columns() {
            let item = StandardListViewItem::from(table_strings[j][i].as_str());
            items.push(item);
        }
        datas.push(items.into());
    }
    return datas;
}

fn main() {
    // test_table();
    let recipe = Example::new().unwrap();
    let recipe_weak = recipe.as_weak();
    recipe.on_button_pressed(button_pressed_handler(recipe_weak));
    recipe.run().unwrap();
}

fn button_pressed_handler(recipe_weak: slint::Weak<Example>) -> impl Fn() {
    move || {
        if let Some(filename) = openfile_name() {
            // get filename and read to Recordbatch
            // println!("{}", filename);
            let rb = read_parquet(&filename, 0, 3).unwrap();

            // init handler
            let recipe = recipe_weak.upgrade().unwrap();

            // get headers
            let headers = generate_headers(&rb);
            recipe.set_headers(headers.into());

            // get body
            let datas = generate_body(&rb);
            recipe.set_datas(datas.into());
        }
    }
}
