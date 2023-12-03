#![windows_subsystem = "windows"]
slint::include_modules!();

use polars::prelude::*;
use slint::{StandardListViewItem, TableColumn, VecModel};
use std::rc::Rc;

fn read_parquet(filename: &str, limit: usize) -> Result<DataFrame, PolarsError> {
    let mut file = std::fs::File::open(filename)?;
    ParquetReader::new(&mut file)
        .with_n_rows(Some(limit))
        .finish()
}

fn read_csv(filename: &str, sep: u8, limit: usize) -> Result<DataFrame, PolarsError> {
    CsvReader::from_path(filename)?
        .with_separator(sep)
        .with_n_rows(Some(limit))
        .finish()
}

fn read_json(filename: &str, limit: usize) -> Result<DataFrame, PolarsError> {
    let mut file = std::fs::File::open(filename)?;
    Ok(JsonReader::new(&mut file).finish()?.head(Some(limit)))
}

fn query_dataframe(df: &DataFrame, sql_str: &str) -> Result<DataFrame, PolarsError> {
    let mut ctx = polars::sql::SQLContext::new();
    ctx.register("CURRENT", df.clone().lazy());
    ctx.execute(sql_str).unwrap().collect()
}

fn openfile_name() -> Result<Option<std::path::PathBuf>, native_dialog::Error> {
    native_dialog::FileDialog::new()
        .reset_location()
        .add_filter("table file", &["parquet", "csv", "json"])
        .show_open_single_file()
}

fn test_table() {
    if let Some(path_buf) = openfile_name().unwrap() {
        let df: DataFrame;
        let filename = path_buf.to_string_lossy().into_owned();
        let file_extension = path_buf.extension().and_then(std::ffi::OsStr::to_str);

        if file_extension == Some("csv") {
            df = read_csv(&filename, b';', 10).unwrap();
        } else if file_extension == Some("parquet") {
            df = read_parquet(&filename, 10).unwrap();
        } else if file_extension == Some("json") {
            df = read_json(&filename, 10).unwrap();
        } else {
            df = DataFrame::empty();
        }
        println!("{}", df);
        let sqled_df = query_dataframe(&df, "SELECT * FROM CURRENT WHERE 成交数量	> 100").unwrap();
        println!("{}", sqled_df);
    }
}

fn main() {
    let recipe = Example::new().unwrap();
    let recipe_weak = recipe.as_weak();
    recipe.on_button_pressed(button_pressed_handler(recipe_weak));
    recipe.run().unwrap();
    // test_table();
}

fn generate_headers(df: &DataFrame) -> Rc<VecModel<TableColumn>> {
    let col_names = df.get_column_names();
    let col_types = df.dtypes();
    let it = col_names.iter().zip(col_types.iter());

    let headers = Rc::new(VecModel::default());
    for (_, (col_name, col_type)) in it.enumerate() {
        let mut header = TableColumn::default();
        header.title = slint::format!("{}({})", col_name, col_type);
        header.min_width = 100.0;
        // println!("{:?}", header);
        headers.push(header);
    }
    return headers;
}

fn generate_body(df: &DataFrame) -> Rc<VecModel<slint::ModelRc<StandardListViewItem>>> {
    let datas = Rc::new(VecModel::default());
    let mut iters = df.iter().map(|s| s.iter()).collect::<Vec<_>>();

    for _ in 0..df.height() {
        let items = Rc::new(VecModel::default());
        for iter in &mut iters {
            let value = iter.next().unwrap();
            let item = StandardListViewItem::from(value.to_string().as_str());
            items.push(item);
        }
        datas.push(items.into());
    }
    return datas;
}

fn button_pressed_handler(recipe_weak: slint::Weak<Example>) -> impl Fn() {
    move || {
        if let Some(path_buf) = openfile_name().unwrap() {
            let df: DataFrame;
            let filename = path_buf.to_string_lossy().into_owned();
            let file_extension = path_buf.extension().and_then(std::ffi::OsStr::to_str);

            if file_extension == Some("csv") {
                df = read_csv(&filename, b';', 10).unwrap();
            } else if file_extension == Some("parquet") {
                df = read_parquet(&filename, 10).unwrap();
            } else if file_extension == Some("json") {
                df = read_json(&filename, 10).unwrap();
            } else {
                df = DataFrame::empty();
            }

            // init handler
            let recipe = recipe_weak.upgrade().unwrap();

            // get headers
            let headers = generate_headers(&df);
            recipe.set_headers(headers.into());

            // get body
            let datas = generate_body(&df);
            recipe.set_datas(datas.into());
        }
    }
}
