from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.silver_bookings",
    comment="Conformed booking fact that joins customers, shows, and payment outcomes for KPI modeling.",
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
        "valid_booking_key": "booking_id IS NOT NULL",
        "valid_customer_reference": "customer_id IS NOT NULL",
        "valid_show_reference": "show_id IS NOT NULL",
        "valid_tickets_count": "tickets_count BETWEEN 1 AND 10",
        "valid_net_amount": "net_amount >= 0",
    }
)
def silver_bookings():
    source = (
        spark.read.table("workspace.bookmyshow_curated.bronze_bookings")
        .withColumn("booked_at_ts", F.to_timestamp("booked_at"))
        .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
    )

    latest_record = Window.partitionBy("booking_id").orderBy(
        F.col("updated_at_ts").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    payments = spark.read.table("workspace.bookmyshow_curated.silver_payments").alias("payments")
    shows = spark.read.table("workspace.bookmyshow_curated.silver_shows").alias("shows")
    customers = spark.read.table("workspace.bookmyshow_curated.silver_customers").alias("customers")

    curated = (
        source.withColumn("row_rank", F.row_number().over(latest_record))
        .filter(F.col("row_rank") == 1)
        .alias("bookings")
        .join(payments, F.col("bookings.booking_id") == F.col("payments.booking_id"), "left")
        .join(shows, F.col("bookings.show_id") == F.col("shows.show_id"), "inner")
        .join(customers, F.col("bookings.customer_id") == F.col("customers.customer_id"), "left")
    )

    return curated.select(
        F.col("bookings.booking_id").alias("booking_id"),
        F.col("bookings.customer_id").alias("customer_id"),
        F.col("bookings.show_id").alias("show_id"),
        F.col("bookings.theater_id").alias("theater_id"),
        F.col("bookings.movie_id").alias("movie_id"),
        F.col("bookings.booked_at_ts").alias("booked_at"),
        F.to_date("bookings.booked_at_ts").alias("booking_date"),
        F.col("bookings.booking_status").alias("booking_status"),
        F.col("bookings.booking_channel").alias("booking_channel"),
        F.col("bookings.coupon_code").alias("coupon_code"),
        F.col("bookings.tickets_count").alias("tickets_count"),
        F.col("bookings.gross_amount").cast("double").alias("gross_amount"),
        F.col("bookings.discount_amount").cast("double").alias("discount_amount"),
        F.col("bookings.convenience_fee").cast("double").alias("convenience_fee"),
        F.col("bookings.net_amount").cast("double").alias("net_amount"),
        F.col("payments.payment_id").alias("payment_id"),
        F.col("payments.payment_ts").alias("payment_ts"),
        F.col("payments.payment_method").alias("payment_method"),
        F.col("payments.gateway_name").alias("gateway_name"),
        F.col("payments.payment_status").alias("payment_status"),
        F.col("payments.transaction_amount").alias("transaction_amount"),
        F.col("payments.refund_amount").alias("refund_amount"),
        F.col("payments.gateway_latency_ms").alias("gateway_latency_ms"),
        F.col("payments.retry_count").alias("retry_count"),
        F.col("payments.net_collected_amount").alias("net_collected_amount"),
        F.col("shows.show_date").alias("show_date"),
        F.col("shows.show_start_ts").alias("show_start_ts"),
        F.col("shows.show_end_ts").alias("show_end_ts"),
        F.col("shows.show_status").alias("show_status"),
        F.col("shows.show_format").alias("show_format"),
        F.col("shows.day_part").alias("day_part"),
        F.col("shows.base_price").alias("base_price"),
        F.col("shows.available_seats").alias("show_capacity"),
        F.col("shows.theater_name").alias("theater_name"),
        F.col("shows.city").alias("city"),
        F.col("shows.state").alias("state"),
        F.col("shows.city_segment").alias("city_segment"),
        F.col("shows.title").alias("movie_title"),
        F.col("shows.genre").alias("genre"),
        F.col("shows.language").alias("language"),
        F.col("shows.content_segment").alias("content_segment"),
        F.col("customers.loyalty_tier").alias("loyalty_tier"),
        F.col("customers.age_band").alias("age_band"),
        F.when((F.col("bookings.booking_status") == "CONFIRMED") & (F.col("payments.payment_status") == "SUCCESS"), F.lit(1)).otherwise(F.lit(0)).alias("is_successful_booking"),
        F.when(F.col("payments.payment_status") == "REFUNDED", F.col("payments.refund_amount")).otherwise(F.lit(0.0)).alias("effective_refund_amount"),
        F.greatest(
            F.when(F.col("payments.payment_status").isin("SUCCESS", "REFUNDED"), F.col("payments.transaction_amount") - F.coalesce(F.col("payments.refund_amount"), F.lit(0.0))).otherwise(F.lit(0.0)),
            F.lit(0.0),
        ).alias("realized_revenue"),
        F.round((F.col("bookings.tickets_count") / F.col("shows.available_seats")) * 100, 2).alias("occupancy_pct_per_booking"),
        F.col("bookings.updated_at_ts").alias("updated_at"),
    )
