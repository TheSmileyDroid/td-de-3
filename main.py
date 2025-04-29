"""
Desafio DE-3 de T&D
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from urllib.request import urlretrieve
from pathlib import Path as P

URL = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json"


def download_file() -> None:
    if P("data.json").exists():
        return
    urlretrieve(URL, "data.json")


def main():
    download_file()
    spark: SparkSession = SparkSession.builder.appName("Health Data").getOrCreate()

    _sc = spark.sparkContext

    dataFrame = spark.read.json("./data.json", allowSingleQuotes=True, multiLine=True)

    dataFrame.printSchema()

    Columns = dataFrame.select(
        F.col("meta")
        .getItem("view")
        .getItem("columns")
        .getItem("fieldName")
        .alias("fieldName"),
    ).withColumn("fieldName", F.explode(F.col("fieldName")))


    Columns.show(truncate=False)

    DataFrame = dataFrame.select(
        F.explode(F.col("data")).alias("completeData")
    )

    for index, column in enumerate(Columns.collect()):
        DataFrame = DataFrame.withColumn(
            f"{column['fieldName']}", F.col("completeData").getItem(index)
        )

    DataFrame = DataFrame.drop("completeData")

    DataFrame.createOrReplaceTempView("health_data")
    DataFrame.createOrReplaceGlobalTempView("health_data")

    with open("output.txt", "w") as sys.stdout:
        DataFrame.show()

        DataFrame.printSchema()
    

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

        sqlDF = spark.sql(sql_query)
        sqlDF.show(truncate=False)

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

        sqlDF = spark.sql(sql_query)
        sqlDF.show(truncate=False)



if __name__ == "__main__":
    main()
