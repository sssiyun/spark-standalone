from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


mysql_host = "MYSQL_HOST"
mysql_port = "MYSQL_PORT"
database_name = "MYSQL_DB"
username = "USERNAME"
password = "PASSWORD"
table = "MY_TABLE"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Python Spark SQL basic example")
        .master("spark://localhost:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config(
            "spark.driver.extraClassPath",
            "/opt/bitnami/spark/jars/mysql-connector-j-8.4.0.jar",
        )
        .getOrCreate()
    )

    # Set log level to INFO
    # spark.sparkContext.setLogLevel("INFO")
    return spark


def read_from_mysql(spark: SparkSession) -> DataFrame:
    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{database_name}"
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    return spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)


def main():
    print("Starting the application")
    spark = create_spark_session()
    print("Spark session created")

    print("Reading data from MySQL")
    df = read_from_mysql(spark)
    print("Data read successfully")

    # Create a view
    df.createOrReplaceTempView("my_view")

    # Perform SQL query
    result_df = spark.sql(f"SELECT * FROM {table} WHERE weight > 15 LIMIT 10")

    # Show the result
    result_df.show()

    print("Stopping the application")
    spark.stop()


if __name__ == "__main__":
    main()
