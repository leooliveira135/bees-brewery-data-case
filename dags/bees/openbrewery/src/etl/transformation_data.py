import os
from src.config.settings import s3_bronze_bucket, s3_silver_bucket, schema
from src.aws.get_credentials import get_aws_connection_info
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date, col, trim

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

def main(spark: SparkSession):
    """
        Main function to orchestrate the data transformation process.
        Args:
            spark (SparkSession): The Spark session.
    """
    transformed_data = transform_data(spark, schema)
    print(f"Total breweries after transformation: {transformed_data.count()}")
    write_to_datalake(transformed_data)

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