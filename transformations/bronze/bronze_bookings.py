from pyspark import pipelines as dp
from pyspark.sql import functions as F


def raw_entity_path(entity_name: str) -> str:
    catalog = spark.conf.get("bookmyshow.catalog", "workspace")
    landing_schema = spark.conf.get("bookmyshow.landing_schema", "bookmyshow_landing")
    raw_volume = spark.conf.get("bookmyshow.raw_volume", "raw_feed")
    return f"/Volumes/{catalog}/{landing_schema}/{raw_volume}/{entity_name}"


@dp.table(
    name="workspace.bookmyshow_curated.bronze_bookings",
    comment="Raw booking transaction data ingested from the governed landing volume.",
    table_properties={
        "quality": "bronze",
        "domain": "bookmyshow",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
    },
)
@dp.expect_or_drop("booking_id_present", "booking_id IS NOT NULL")
@dp.expect_or_drop("show_id_present", "show_id IS NOT NULL")
def bronze_bookings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("dropFieldIfAllNull", "true")
        .option("multiLine", "false")
        .load(raw_entity_path("bookings"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
