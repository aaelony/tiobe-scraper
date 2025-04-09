use chrono::Local;
use polars::prelude::*;
use scraper::{Html, Selector};
use std::{collections::HashMap, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "https://www.tiobe.com/tiobe-index/";
    let body = reqwest::get(url).await?.text().await?;
    let document = Html::parse_document(&body);
    let table_selector = Selector::parse("table").unwrap();
    let tables = document.select(&table_selector);

    let timestamp = Local::now().format("%Y%m%d_%H%M").to_string();

    for (table_idx, table) in tables.enumerate() {
        let headers: Vec<String> = table
            .select(&Selector::parse("th").unwrap())
            .map(|th| th.text().collect::<String>().trim().to_string())
            .collect();

        let rows: Vec<Vec<String>> = table
            .select(&Selector::parse("tr").unwrap())
            .map(|tr| {
                tr.select(&Selector::parse("td").unwrap())
                    .map(|td| td.text().collect::<String>().trim().to_string())
                    .collect()
            })
            .filter(|row: &Vec<String>| !row.is_empty())
            .collect();

        if !rows.is_empty() {
            let mut columns: Vec<Column> = Vec::new();
            let mut seen_names: HashMap<String, i32> = HashMap::new();

            for (col_idx, header) in headers.iter().enumerate() {
                let column_data: Vec<String> = rows
                    .iter()
                    .map(|row| row.get(col_idx).cloned().unwrap_or_default())
                    .collect();

                let base_header_name = header.as_str();
                let col_counter = seen_names
                    .entry(base_header_name.clone().into())
                    .or_insert(0);
                *col_counter += 1;
                let header_name = if *col_counter > 1 {
                    format!("{}_{}", base_header_name, col_counter)
                } else {
                    base_header_name.into()
                };
                columns.push(Column::new(header_name.into(), column_data));
            }

            let mut df = DataFrame::new(columns).unwrap();
            let filename = format!("out/tiobe-rankings-{}-{}.tsv", table_idx + 1, timestamp);

            let mut file = std::fs::File::create(&filename)?;
            CsvWriter::new(&mut file)
                .include_header(true)
                .with_separator(b'\t')
                .finish(&mut df)?;
            println!("\nTable {}", table_idx + 1);
            println!("{:?}", df);
        }
    }

    Ok(())
}
