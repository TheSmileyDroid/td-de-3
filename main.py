"""
Desafio DE-3 de T&D
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from urllib.request import urlretrieve
from pathlib import Path as P
from pyspark.sql import DataFrame as SparkDataFrame

URL = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json"


def download_file() -> None:
    """Download data file if it doesn't exist.
    
    Downloads the data file from the specified URL if it 
    doesn't already exist locally.
    """
    if P("data.json").exists():
        return
    urlretrieve(URL, "data.json")


def dataframe_to_markdown(df: SparkDataFrame, limit: int = 20) -> str:
    """Convert Spark DataFrame to markdown table format.
    
    Args:
        df: The Spark DataFrame to convert
        limit: Maximum number of rows to include (default: 20)
        
    Returns:
        String containing the DataFrame as a markdown table
    """
    rows = df.limit(limit).collect()
    if not rows:
        return "*Empty table*"
    
    columns = df.columns
    header = " | ".join(columns)
    separator = " | ".join(["---"] * len(columns))
    
    md_rows = []
    for row in rows:
        values = [str(row[col]) for col in columns]
        md_rows.append(" | ".join(values))
    
    markdown_table = f"| {header} |\n| {separator} |\n"
    markdown_table += "\n".join([f"| {row} |" for row in md_rows])
    
    if df.count() > limit:
        markdown_table += f"\n\n*Showing {limit} of {df.count()} rows*"
    
    return markdown_table


def main():
    download_file()
    spark: SparkSession = SparkSession.builder.appName("Health Data").getOrCreate()

    _sc = spark.sparkContext

    dataFrame = spark.read.json("./data.json", allowSingleQuotes=True, multiLine=True)

    dataFrame.printSchema()

    columns = dataFrame.select(
        F.col("meta")
        .getItem("view")
        .getItem("columns")
        .getItem("fieldName")
        .alias("fieldName"),
    ).withColumn("fieldName", F.explode(F.col("fieldName")))


    columns.show(truncate=False)

    data_frame = dataFrame.select(
        F.explode(F.col("data")).alias("completeData")
    )

    for index, column in enumerate(columns.collect()):
        data_frame = data_frame.withColumn(
            f"{column['fieldName']}", F.col("completeData").getItem(index)
        )

    data_frame = data_frame.drop("completeData")

    data_frame.createOrReplaceTempView("health_data")
    data_frame.createOrReplaceGlobalTempView("health_data")

    with open("output.txt", "w") as sys.stdout:
        print("## Data Preview")
        print(dataframe_to_markdown(data_frame))
        print("\n## Schema")
        print("```")
        data_frame.printSchema()
        print("```")
    
        # Exemplo de consulta SQL
        sql_query = """
        SELECT
            MAX(year) AS max_year,
            MIN(year) AS min_year,
            COUNT(*) AS total_records,
            COUNT(DISTINCT county) AS total_counties
        FROM
            health_data
        """

        sql_df = spark.sql(sql_query)
        print("\n## Summary Statistics")
        print(dataframe_to_markdown(sql_df))

        sql_query = """
        SELECT
            COUNT(*) AS total_per_year,
            year
        FROM
            health_data
        GROUP BY
            year
        ORDER BY
            year
        """

        sql_df = spark.sql(sql_query)
        print("\n## Records per Year")
        print(dataframe_to_markdown(sql_df))


if __name__ == "__main__":
    main()
