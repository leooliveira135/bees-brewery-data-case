from pyspark.sql.types import StructType, StructField, StringType

endpoint = f"https://api.openbrewerydb.org/v1/breweries?page={{page}}&per_page=200"

s3_bronze_bucket = 'bees-openbrewerydb-bronze'
s3_silver_bucket = 'bees-openbrewerydb-silver'
s3_gold_bucket = 'bees-openbrewerydb-gold'

schema = StructType([
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("id", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("state", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("street", StringType(), True),
    StructField("website_url", StringType(), True),
])

aws_glue_role = "glue-crawler-role"

athena_output_queries = "s3://bees-openbrewerydb-gold/athena/"