import duckdb


class ParquetReader:
    def __init__(self, filename: str):
        self.filename_ = filename
        self.conn_ = duckdb.connect(database=":memory:")
        self.duckdb_datatype_map_ = {
            "VARCHAR": "str",
            "INTEGER": "i32",
            "UINTEGER": "u32",
            "TINYINT": "i8",
            "UTINYINT": "u8",
            "DOUBLE": "f64",
            "REAL": "f32",
            "BIGINT": "i64",
            "UBIGINT": "u64",
        }

    def read_schema(self) -> dict:
        """Return: {column_name: column_type}"""
        desc_data = self.conn_.execute(f"DESCRIBE SELECT * FROM '{self.filename_}';").fetchall()
        # print(desc_data)
        return {item[0]: self.duckdb_datatype_map_.get(item[1], item[1]) for item in desc_data}

    def read_data(self, fake_sql: str = "SELECT * FROM CURRENT LIMIT 10") -> list[tuple]:
        """Return: [(col1, col2, ...), ...]"""
        real_sql = fake_sql.replace("CURRENT", f"read_parquet('{self.filename_}')")
        parquet_data = self.conn_.execute(real_sql).fetchall()
        self.conn_.close()
        return parquet_data


if __name__ == "__main__":
    obj = ParquetReader("20231108-trade.parquet")
    print(obj.read_schema())
    print(obj.read_data())
