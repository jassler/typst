use crate::diag::StrResult;
use crate::engine::Engine;
use crate::foundations::{Array, Bytes, IntoValue, Str, Value, func};
use ecow::eco_format;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};

/// Reads structured data from a CSV file.
///
/// The CSV file will be read and parsed into a 2-dimensional array of strings:
/// Each row in the CSV file will be represented as an array of strings, and all
/// rows will be collected into a single array. Header rows will not be
/// stripped.
///
/// # Example
/// ```example
/// #let results = csv("example.csv")
///
/// #table(
///   columns: 2,
///   [*Condition*], [*Result*],
///   ..results.flatten(),
/// )
/// ```
#[func(title = "SQL")]
pub fn sql(
    engine: &mut Engine,
    /// A [path]($syntax/#paths) to a SQLite file.
    source: Str,
    /// The SQL query to execute
    query: Str,
) -> StrResult<Array> {
    let conn =
        Connection::open_with_flags(source.to_string(), OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|err| {
                eco_format!("failed to establish connection to the database ({err})")
            })?;
    let mut stmt = conn
        .prepare(&query)
        .map_err(|err| eco_format!("failed to compile sql query ({err})"))?;
    let col_count = stmt.column_count();
    let mut rows = stmt
        .query([])
        .map_err(|err| eco_format!("failed to run sql query ({err})"))?;

    let mut array = Array::new();

    while let Some(row) = rows
        .next()
        .map_err(|err| eco_format!("failed to read row from sql query ({err})"))?
    {
        let mut one = Array::new();
        for i in 0..col_count {
            let v = match row.get_ref(i).unwrap() {
                ValueRef::Null => Value::None,
                ValueRef::Integer(x) => x.into_value(),
                ValueRef::Real(x) => x.into_value(),
                ValueRef::Text(x) => String::from_utf8_lossy(x).into_owned().into_value(),
                ValueRef::Blob(x) => Bytes::new(x.to_vec()).into_value(),
            };
            one.push(v);
        }
        array.push(Value::Array(one));
    }

    Ok(array)
}
