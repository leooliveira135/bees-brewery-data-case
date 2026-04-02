from src.config.settings import s3_silver_bucket
from pyspark.sql import SparkSession, DataFrame

def read_data(spark: SparkSession):
    """
        Reads the transformed brewery data from the S3 silver bucket in Delta Lake format.
        Args:
            spark (SparkSession): The Spark session.
        Returns:
            DataFrame: A Spark DataFrame containing the transformed brewery data read from the S3 silver bucket.
    """
    df = spark.read.format("delta").load(f"s3a://{s3_silver_bucket}/openbrewery_db")
    return df

def aggregate_data(df: DataFrame):
    """
        Aggregates the brewery data by state, counting the number of breweries in each state.
        Args:
            df (DataFrame): The Spark DataFrame containing the transformed brewery data.
        Returns:
            DataFrame: A Spark DataFrame containing the aggregated brewery data, with columns for state and brewery count.
    """
    aggregated_df = df.groupBy("brewery_type", "country", "state").agg({"*": "count"}.alias("brewery_count"))
    print(f"Total states with breweries: {aggregated_df.count()}")
    print("Sample of aggregated data:")
    aggregated_df.show(5)
    return aggregated_df

def write_aggregated_data(df: DataFrame):
    """
        Writes the aggregated brewery data to the S3 silver bucket in Delta Lake format.
        Args:
            df (DataFrame): The Spark DataFrame containing the aggregated brewery data.
    """
    df.write.format("delta").mode("overwrite").partitionBy("brewery_type", "country").save(f"s3a://{s3_silver_bucket}/openbrewery_aggregated_db")

def main(spark: SparkSession):
    """
        Main function to orchestrate the data aggregation process.
        Args:
            spark (SparkSession): The Spark session.
    """
    transformed_data = read_data(spark)
    aggregated_data = aggregate_data(transformed_data)
    write_aggregated_data(aggregated_data)

if __name__ == "__main__":
    spark = SparkSession.builder \
                        .appName("Bees Data Aggregation from Open Brewery DB") \
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