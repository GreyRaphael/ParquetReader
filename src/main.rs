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
    // let recipe = Example::new().unwrap();
    // let recipe_weak = recipe.as_weak();
    // recipe.on_button_pressed(button_pressed_handler(recipe_weak));
    // recipe.run().unwrap();
    test_table();
}
