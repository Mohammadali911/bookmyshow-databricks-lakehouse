from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.gold_executive_kpis",
    comment="Executive-ready KPI snapshot for BookMyShow-style revenue, occupancy, payment quality, and customer reach.",
    table_properties={
        "quality": "gold",
        "domain": "bookmyshow",
    },
)
@dp.expect("positive_ticket_volume", "tickets_sold >= 0")
def gold_executive_kpis():
    bookings = spark.read.table("workspace.bookmyshow_curated.silver_bookings")
    shows = spark.read.table("workspace.bookmyshow_curated.silver_shows")

    booking_kpis = bookings.agg(
        F.max("booking_date").alias("latest_booking_date"),
        F.countDistinct("booking_id").alias("total_bookings"),
        F.sum(F.when(F.col("is_successful_booking") == 1, F.col("tickets_count")).otherwise(F.lit(0))).alias("tickets_sold"),
        F.sum(F.col("realized_revenue")).alias("realized_revenue"),
        F.sum(F.col("effective_refund_amount")).alias("refund_amount"),
        F.countDistinct(F.when(F.col("is_successful_booking") == 1, F.col("customer_id"))).alias("active_customers"),
        F.avg(F.when(F.col("payment_id").isNotNull(), F.col("gateway_latency_ms"))).alias("avg_gateway_latency_ms"),
        F.avg(F.when(F.col("payment_id").isNotNull(), F.col("retry_count"))).alias("avg_retry_count"),
        F.sum(F.when(F.col("payment_status") == "FAILED", F.lit(1)).otherwise(F.lit(0))).alias("failed_payment_count"),
        F.sum(F.when(F.col("payment_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("payment_attempt_count"),
    )

    show_kpis = shows.agg(
        F.countDistinct("show_id").alias("total_shows"),
        F.sum(F.when(F.col("show_status") == "CANCELLED", F.lit(1)).otherwise(F.lit(0))).alias("cancelled_shows"),
        F.sum(F.col("available_seats")).alias("total_capacity"),
        F.countDistinct("theater_id").alias("active_theaters"),
    )

    return booking_kpis.crossJoin(show_kpis).select(
        "latest_booking_date",
        "total_bookings",
        "tickets_sold",
        F.round("realized_revenue", 2).alias("realized_revenue"),
        F.round("refund_amount", 2).alias("refund_amount"),
        "active_customers",
        "active_theaters",
        "total_shows",
        "cancelled_shows",
        F.round((F.col("tickets_sold") / F.greatest(F.col("total_capacity"), F.lit(1))) * 100, 2).alias("network_occupancy_pct"),
        F.round((F.col("cancelled_shows") / F.greatest(F.col("total_shows"), F.lit(1))) * 100, 2).alias("show_cancellation_rate_pct"),
        F.round((F.col("total_bookings") / F.greatest(F.col("active_customers"), F.lit(1))), 2).alias("bookings_per_active_customer"),
        F.round((F.col("failed_payment_count") / F.greatest(F.col("payment_attempt_count"), F.lit(1))) * 100, 2).alias("payment_failure_rate_pct"),
        F.round("avg_gateway_latency_ms", 2).alias("avg_gateway_latency_ms"),
        F.round("avg_retry_count", 2).alias("avg_retry_count"),
    )
