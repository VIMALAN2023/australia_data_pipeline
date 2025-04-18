# data loading in postgres

from pyspark.sql import SparkSession

# Initialize Spark session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("Load Data into PostgreSQL") \
    .config("spark.jars", "postgresql-42.7.5.jar") \
    .config("spark.driver.extraClassPath", "postgresql-42.7.5.jar") \
    .getOrCreate()

# JDBC connection properties
url = "jdbc:postgresql://localhost:5432/test_db"
properties = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# Load Common Crawl data in JSONL format
common_crawl_data = spark.read.json(r"C:\Users\Vimalan\Downloads\common\aus_output.jsonl")

# Load ABR data (CSV)
abr_data = spark.read.option("header", "true").csv(r"C:\Users\Vimalan\Downloads\common\abr_bulk_extract_10.csv")

# Rename columns for easier access in ABR data
abr_data = abr_data.withColumnRenamed("ABN (Australian Business Number)", "abn") \
                   .withColumnRenamed("Entity Name", "entity_name") \
                   .withColumnRenamed("Entity Type", "entity_type") \
                   .withColumnRenamed("Entity Status", "entity_status") \
                   .withColumnRenamed("Entity Address", "entity_address") \
                   .withColumnRenamed("Entity Postcode", "entity_postcode") \
                   .withColumnRenamed("Entity State", "entity_state") \
                   .withColumnRenamed("Entity Start Date", "entity_start_date")

# Perform any necessary transformations or renaming for Common Crawl data
common_crawl_data = common_crawl_data.select("industry", "title", "url")

# Perform any necessary transformations or renaming for ABR data
abr_data = abr_data.select("abn", "entity_name", "entity_type", "entity_status", 
                           "entity_address", "entity_postcode", "entity_state", "entity_start_date")

# Write the Common Crawl data to PostgreSQL (overwrite mode)
common_crawl_data.write.jdbc(url=url, table="common_crawl_data", mode="overwrite", properties=properties)

# Write the ABR data to PostgreSQL (overwrite mode)
abr_data.write.jdbc(url=url, table="abr_data", mode="overwrite", properties=properties)

print("Data ingestion into PostgreSQL completed successfully.")
