from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.gold_revenue_daily",
    comment="Daily revenue performance by city, genre, language, and booking channel.",
    table_properties={
        "quality": "gold",
        "domain": "bookmyshow",
    },
)
@dp.expect("non_negative_realized_revenue", "realized_revenue >= 0")
def gold_revenue_daily():
    bookings = spark.read.table("workspace.bookmyshow_curated.silver_bookings")

    aggregated = bookings.groupBy(
        "booking_date",
        "city",
        "state",
        "city_segment",
        "genre",
        "language",
        "booking_channel",
    ).agg(
        F.countDistinct("booking_id").alias("bookings_count"),
        F.sum(F.when(F.col("is_successful_booking") == 1, F.col("tickets_count")).otherwise(F.lit(0))).alias("tickets_sold"),
        F.sum(F.when(F.col("booking_status") == "CONFIRMED", F.col("gross_amount")).otherwise(F.lit(0.0))).alias("gross_booking_value"),
        F.sum(F.when(F.col("is_successful_booking") == 1, F.col("convenience_fee")).otherwise(F.lit(0.0))).alias("convenience_fee_revenue"),
        F.sum(F.col("discount_amount")).alias("discount_burn"),
        F.sum(F.col("effective_refund_amount")).alias("refund_amount"),
        F.sum(F.col("realized_revenue")).alias("realized_revenue"),
        F.sum(F.when(F.col("is_successful_booking") == 1, F.lit(1)).otherwise(F.lit(0))).alias("successful_bookings"),
        F.sum(F.when(F.col("booking_status") == "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("cancelled_bookings"),
    )

    return aggregated.select(
        "booking_date",
        "city",
        "state",
        "city_segment",
        "genre",
        "language",
        "booking_channel",
        "bookings_count",
        "tickets_sold",
        F.round("gross_booking_value", 2).alias("gross_booking_value"),
        F.round("convenience_fee_revenue", 2).alias("convenience_fee_revenue"),
        F.round("discount_burn", 2).alias("discount_burn"),
        F.round("refund_amount", 2).alias("refund_amount"),
        F.round("realized_revenue", 2).alias("realized_revenue"),
        "successful_bookings",
        "cancelled_bookings",
        F.round(F.col("realized_revenue") / F.greatest(F.col("successful_bookings"), F.lit(1)), 2).alias("avg_order_value"),
    )
