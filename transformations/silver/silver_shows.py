from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.silver_shows",
    comment="Conformed show schedule with enriched theater and movie context.",
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
        "valid_show_key": "show_id IS NOT NULL",
        "valid_show_date": "show_date IS NOT NULL",
        "valid_show_window": "show_end_ts > show_start_ts",
        "valid_available_seats": "available_seats > 0",
    }
)
def silver_shows():
    source = (
        spark.read.table("workspace.bookmyshow_curated.bronze_shows")
        .withColumn("show_date_parsed", F.to_date("show_date"))
        .withColumn("show_start_ts_parsed", F.to_timestamp("show_start_ts"))
        .withColumn("show_end_ts_parsed", F.to_timestamp("show_end_ts"))
        .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
    )

    latest_record = Window.partitionBy("show_id").orderBy(
        F.col("updated_at_ts").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    theaters = spark.read.table("workspace.bookmyshow_curated.silver_theaters")
    movies = spark.read.table("workspace.bookmyshow_curated.silver_movies")

    return (
        source.withColumn("row_rank", F.row_number().over(latest_record))
        .filter(F.col("row_rank") == 1)
        .join(theaters, on="theater_id", how="inner")
        .join(movies, on="movie_id", how="inner")
        .select(
            "show_id",
            "show_date_parsed",
            "show_start_ts_parsed",
            "show_end_ts_parsed",
            "show_status",
            "show_format",
            "show_language",
            "base_price",
            "available_seats",
            "theater_id",
            "theater_name",
            "city",
            "state",
            "city_segment",
            "screens_count",
            "screen_id",
            "seat_capacity",
            "avg_seats_per_screen",
            "movie_id",
            "title",
            "genre",
            "language",
            "duration_mins",
            "certification",
            "content_segment",
            F.when(F.dayofweek("show_date_parsed").isin([1, 7]), F.lit(True)).otherwise(F.lit(False)).alias("is_weekend_show"),
            F.when(F.hour("show_start_ts_parsed") < 12, F.lit("morning"))
            .when(F.hour("show_start_ts_parsed") < 17, F.lit("afternoon"))
            .when(F.hour("show_start_ts_parsed") < 21, F.lit("prime_time"))
            .otherwise(F.lit("late_night")).alias("day_part"),
            ((F.unix_timestamp("show_end_ts_parsed") - F.unix_timestamp("show_start_ts_parsed")) / 60).cast("int").alias("scheduled_duration_mins"),
            F.col("updated_at_ts").alias("updated_at"),
        )
        .withColumnRenamed("show_date_parsed", "show_date")
        .withColumnRenamed("show_start_ts_parsed", "show_start_ts")
        .withColumnRenamed("show_end_ts_parsed", "show_end_ts")
    )
