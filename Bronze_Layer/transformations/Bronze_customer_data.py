from pyspark.sql.functions import current_timestamp, col
import logging

# --------------------------------------------------
# Logger Configuration
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_ingestion")

# --------------------------------------------------
# Configuration
# --------------------------------------------------

SOURCE_PATH = "s3://bank-customer-churn-data/processed/"
BRONZE_TABLE = "churn_catalog.raw.customer_data"

try:

    logger.info("Starting Bronze ingestion pipeline")

    # --------------------------------------------------
    # Check if source path exists and contains files
    # --------------------------------------------------

    try:
        files = dbutils.fs.ls(SOURCE_PATH)
    except Exception:
        logger.warning(f"Source path does not exist: {SOURCE_PATH}")
        files = []

    if len(files) == 0:
        logger.warning("No files found in source path. Bronze ingestion skipped.")

    else:

        logger.info(f"{len(files)} file(s) detected")

        # --------------------------------------------------
        # Read raw parquet data from S3
        # --------------------------------------------------

        df = spark.read.format("parquet").load(SOURCE_PATH)

        logger.info("Raw data loaded successfully")

        # --------------------------------------------------
        # Normalize column names (avoid case issues)
        # --------------------------------------------------

        df = df.toDF(*[c.lower() for c in df.columns])

        logger.info("Column normalization completed")

        # --------------------------------------------------
        # Add Bronze metadata
        # --------------------------------------------------

        df_bronze = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", col("_metadata.file_path"))

        logger.info("Metadata columns added")

        # --------------------------------------------------
        # Ensure Bronze schema exists
        # --------------------------------------------------

        spark.sql("CREATE SCHEMA IF NOT EXISTS churn_catalog.raw")

        logger.info("Schema verified")

        # --------------------------------------------------
        # Write data to Bronze Delta table
        # --------------------------------------------------

        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(BRONZE_TABLE)

        logger.info(f"Bronze table updated successfully: {BRONZE_TABLE}")

except Exception as e:

    logger.error("Bronze ingestion failed")
    logger.error(str(e))

finally:

    logger.info("Bronze pipeline execution finished")