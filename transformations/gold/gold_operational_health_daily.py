from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.gold_operational_health_daily",
    comment="Daily theater operational health metrics built from show supply and booking demand.",
    table_properties={
        "quality": "gold",
        "domain": "bookmyshow",
    },
)
@dp.expect("valid_show_volume", "total_shows >= cancelled_shows")
def gold_operational_health_daily():
    shows = spark.read.table("workspace.bookmyshow_curated.silver_shows")
    bookings = spark.read.table("workspace.bookmyshow_curated.silver_bookings")

    booking_by_show = bookings.groupBy("show_id").agg(
        F.sum(F.when(F.col("is_successful_booking") == 1, F.col("tickets_count")).otherwise(F.lit(0))).alias("booked_seats"),
        F.sum(F.col("realized_revenue")).alias("realized_revenue"),
        F.countDistinct(F.when(F.col("is_successful_booking") == 1, F.col("booking_id"))).alias("paid_bookings"),
        F.sum(F.col("effective_refund_amount")).alias("refund_amount"),
    )

    show_health = (
        shows.join(booking_by_show, on="show_id", how="left")
        .fillna({"booked_seats": 0, "realized_revenue": 0.0, "paid_bookings": 0, "refund_amount": 0.0})
        .withColumn(
            "show_occupancy_pct",
            F.round((F.col("booked_seats") / F.greatest(F.col("available_seats"), F.lit(1))) * 100, 2),
        )
    )

    aggregated = show_health.groupBy(
        "show_date",
        "theater_id",
        "theater_name",
        "city",
        "state",
        "city_segment",
    ).agg(
        F.countDistinct("show_id").alias("total_shows"),
        F.sum(F.when(F.col("show_status") == "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("cancelled_shows"),
        F.sum(F.when(F.col("paid_bookings") == 0, F.lit(1)).otherwise(F.lit(0))).alias("shows_without_bookings"),
        F.sum(F.col("available_seats")).alias("total_capacity"),
        F.sum(F.col("booked_seats")).alias("booked_seats"),
        F.avg(F.col("show_occupancy_pct")).alias("avg_show_occupancy_pct"),
        F.sum(F.col("realized_revenue")).alias("realized_revenue"),
        F.sum(F.col("refund_amount")).alias("refund_amount"),
    )

    return aggregated.select(
        "show_date",
        "theater_id",
        "theater_name",
        "city",
        "state",
        "city_segment",
        "total_shows",
        "cancelled_shows",
        "shows_without_bookings",
        "total_capacity",
        "booked_seats",
        F.round((F.col("booked_seats") / F.greatest(F.col("total_capacity"), F.lit(1))) * 100, 2).alias("occupancy_pct"),
        F.round((F.col("cancelled_shows") / F.greatest(F.col("total_shows"), F.lit(1))) * 100, 2).alias("show_cancellation_rate_pct"),
        F.round(F.col("avg_show_occupancy_pct"), 2).alias("avg_show_occupancy_pct"),
        F.round("realized_revenue", 2).alias("realized_revenue"),
        F.round("refund_amount", 2).alias("refund_amount"),
    )
