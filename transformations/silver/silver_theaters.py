from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.silver_theaters",
    comment="Conformed theater dimension with deduplication and business-friendly attributes.",
    table_properties={
        "quality": "silver",
        "domain": "bookmyshow",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_theater_key": "theater_id IS NOT NULL",
        "valid_screens_count": "screens_count > 0",
        "valid_seat_capacity": "seat_capacity > 0",
        "valid_opened_on": "opened_on IS NOT NULL",
    }
)
def silver_theaters():
    source = (
        spark.read.table("workspace.bookmyshow_curated.bronze_theaters")
        .withColumn("opened_on_date", F.to_date("opened_on"))
        .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
    )

    latest_record = Window.partitionBy("theater_id").orderBy(
        F.col("updated_at_ts").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    metro_cities = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkata"]

    return (
        source.withColumn("row_rank", F.row_number().over(latest_record))
        .filter(F.col("row_rank") == 1)
        .select(
            "theater_id",
            "theater_name",
            "city",
            "state",
            "screens_count",
            "seat_capacity",
            F.round(F.col("seat_capacity") / F.col("screens_count"), 2).alias("avg_seats_per_screen"),
            F.col("opened_on_date").alias("opened_on"),
            F.col("is_active").alias("theater_is_active"),
            F.when(F.col("city").isin(metro_cities), F.lit("metro")).otherwise(F.lit("growth_market")).alias("city_segment"),
            F.col("updated_at_ts").alias("updated_at"),
        )
    )
