import logging
from src.setup.settings import s3_bronze_bucket, s3_silver_bucket, schema, aws_glue_role
from src.aws.glue_catalog import create_glue_database, create_glue_crawler, start_glue_crawler, list_glue_db_tables
from src.aws.airflow_connection import get_aws_connection_info
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date, col

def transform_data(spark, schema=None):
    """
        Transforms the raw brewery data into a Spark DataFrame, adding an ingestion date column.
        Args:
            spark (SparkSession): The Spark session.
            schema (StructType): The schema to be applied to the DataFrame, defining the structure and data types of the columns.
        Returns:
            DataFrame: A Spark DataFrame containing the transformed brewery data with an additional ingestion date column.
    """
    df = spark.read.schema(schema).json(f"s3a://{s3_bronze_bucket}/openbrewery_data.json")
    df = df.withColumn("ingestion_date", current_date())
    df = df.withColumn("latitude", col("latitude").cast("double"))
    df = df.withColumn("longitude", col("longitude").cast("double"))
    return df

def write_to_datalake(df: DataFrame):
    """
        Writes the transformed brewery data to the S3 bronze bucket in Delta Lake format.
        Args:
            df (DataFrame): The Spark DataFrame containing the transformed brewery data.
    """
    df.write.format("delta").mode("overwrite").partitionBy(["country", "state"]).save(f"s3a://{s3_silver_bucket}/openbrewery_db")

def create_glue_silver_catalog():
    """
        Creates a Glue Catalog table for the transformed brewery data in the silver layer.
        This function defines the schema for the transformed brewery data and creates a Glue Catalog table in the specified database. The table is created with the appropriate columns and data types to store the transformed brewery information effectively. This allows for easy querying and analysis of the transformed brewery data using AWS Glue and Athena.
    """
    _, _, region_name = get_aws_connection_info()
    create_glue_database(database_name="openbrewery_silver_db", aws_region=region_name)
    create_glue_crawler(
        crawler_name="openbrewery_silver_crawler",
        role_arn=aws_glue_role,
        database_name="openbrewery_silver_db",
        s3_path=f"s3a://{s3_silver_bucket}/openbrewery_db",
        aws_region=region_name
    )
    start_glue_crawler(crawler_name="openbrewery_silver_crawler", aws_region=region_name)
    tables = list_glue_db_tables(database_name="openbrewery_silver_db", aws_region=region_name)
    logging.info(f"Tables in Glue database 'openbrewery_silver_db': {tables}")

def main(spark: SparkSession):
    """
        Main function to orchestrate the data transformation process.
        Args:
            spark (SparkSession): The Spark session.
    """
    transformed_data = transform_data(spark, schema)
    logging.info(f"Total breweries after transformation: {transformed_data.count()}")
    write_to_datalake(transformed_data)
    create_glue_silver_catalog()

if __name__ == "__main__":

    spark = SparkSession.builder \
                        .appName("Bees Data Processing from Open Brewery DB") \
                        .config(
                            "spark.jars.packages",
                            ",".join([
                                "io.delta:delta-spark_2.12:3.1.0",
                                "org.apache.hadoop:hadoop-aws:3.3.4",
                                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
                            ])
                        ) \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
                        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                        .config("spark.driver.memory", "6g") \
                        .config("spark.driver.maxResultSize", "2g") \
                        .config("spark.sql.shuffle.partitions", "8") \
                        .getOrCreate()

    main(spark)

    spark.stop()