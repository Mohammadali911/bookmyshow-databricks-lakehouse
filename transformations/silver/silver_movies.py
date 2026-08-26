from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.silver_movies",
    comment="Conformed movie dimension with release, genre, and commercial segmentation attributes.",
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
        "valid_movie_key": "movie_id IS NOT NULL",
        "valid_duration": "duration_mins BETWEEN 60 AND 300",
        "valid_rating": "imdb_rating BETWEEN 0 AND 10",
        "valid_release_date": "release_date IS NOT NULL",
    }
)
def silver_movies():
    source = (
        spark.read.table("workspace.bookmyshow_curated.bronze_movies")
        .withColumn("release_date_parsed", F.to_date("release_date"))
        .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
    )

    latest_record = Window.partitionBy("movie_id").orderBy(
        F.col("updated_at_ts").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    return (
        source.withColumn("row_rank", F.row_number().over(latest_record))
        .filter(F.col("row_rank") == 1)
        .select(
            "movie_id",
            "title",
            "genre",
            "language",
            "duration_mins",
            "certification",
            F.col("release_date_parsed").alias("release_date"),
            F.col("imdb_rating").cast("double").alias("imdb_rating"),
            "movie_format",
            "distributor",
            F.when(F.col("imdb_rating") >= 8.0, F.lit("high_acclaim"))
            .when(F.col("imdb_rating") >= 6.5, F.lit("mass_market"))
            .otherwise(F.lit("niche")).alias("content_segment"),
            F.when(F.datediff(F.to_date("updated_at_ts"), F.col("release_date_parsed")) <= 30, F.lit("new_release"))
            .when(F.datediff(F.to_date("updated_at_ts"), F.col("release_date_parsed")) <= 120, F.lit("recent_release"))
            .otherwise(F.lit("catalog_title")).alias("release_window"),
            F.col("updated_at_ts").alias("updated_at"),
        )
    )
