from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.gold_occupancy_daily",
    comment="Daily occupancy performance by theater, movie, and day part.",
    table_properties={
        "quality": "gold",
        "domain": "bookmyshow",
    },
)
@dp.expect("valid_capacity", "total_capacity >= booked_seats")
def gold_occupancy_daily():
    shows = spark.read.table("workspace.bookmyshow_curated.silver_shows")
    bookings = spark.read.table("workspace.bookmyshow_curated.silver_bookings")

    booking_by_show = bookings.groupBy("show_id").agg(
        F.sum(F.when(F.col("is_successful_booking") == 1, F.col("tickets_count")).otherwise(F.lit(0))).alias("booked_seats"),
        F.sum(F.col("realized_revenue")).alias("realized_revenue"),
        F.countDistinct(F.when(F.col("is_successful_booking") == 1, F.col("booking_id"))).alias("paid_bookings"),
    )

    occupancy_base = (
        shows.join(booking_by_show, on="show_id", how="left")
        .fillna({"booked_seats": 0, "realized_revenue": 0.0, "paid_bookings": 0})
    )

    aggregated = occupancy_base.groupBy(
        "show_date",
        "theater_id",
        "theater_name",
        "city",
        "state",
        "movie_id",
        "title",
        "genre",
        "language",
        "day_part",
    ).agg(
        F.countDistinct("show_id").alias("shows_scheduled"),
        F.sum(F.when(F.col("show_status") == "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("shows_cancelled"),
        F.sum(F.col("available_seats")).alias("total_capacity"),
        F.sum(F.col("booked_seats")).alias("booked_seats"),
        F.sum(F.col("paid_bookings")).alias("paid_bookings"),
        F.sum(F.col("realized_revenue")).alias("realized_revenue"),
    )

    return aggregated.select(
        "show_date",
        "theater_id",
        "theater_name",
        "city",
        "state",
        "movie_id",
        F.col("title").alias("movie_title"),
        "genre",
        "language",
        "day_part",
        "shows_scheduled",
        "shows_cancelled",
        "total_capacity",
        "booked_seats",
        "paid_bookings",
        F.round((F.col("booked_seats") / F.greatest(F.col("total_capacity"), F.lit(1))) * 100, 2).alias("occupancy_pct"),
        F.round((F.col("realized_revenue") / F.greatest(F.col("booked_seats"), F.lit(1))), 2).alias("revenue_per_attendee"),
        F.round("realized_revenue", 2).alias("realized_revenue"),
    )
