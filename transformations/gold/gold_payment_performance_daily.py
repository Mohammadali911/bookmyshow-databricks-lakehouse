from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.gold_payment_performance_daily",
    comment="Daily payment gateway and method performance metrics for payment operations reporting.",
    table_properties={
        "quality": "gold",
        "domain": "bookmyshow",
    },
)
@dp.expect("valid_attempt_count", "payment_attempts >= successful_payments")
def gold_payment_performance_daily():
    payments = spark.read.table("workspace.bookmyshow_curated.silver_bookings").filter(F.col("payment_id").isNotNull())

    aggregated = payments.groupBy(
        F.to_date("payment_ts").alias("payment_date"),
        "gateway_name",
        "payment_method",
        "booking_channel",
        "city_segment",
    ).agg(
        F.countDistinct("payment_id").alias("payment_attempts"),
        F.sum(F.col("is_successful_booking")).alias("successful_payments"),
        F.sum(F.when(F.col("payment_status") == "FAILED", F.lit(1)).otherwise(F.lit(0))).alias("failed_payments"),
        F.sum(F.when(F.col("payment_status") == "PENDING", F.lit(1)).otherwise(F.lit(0))).alias("pending_payments"),
        F.sum(F.when(F.col("payment_status") == "REFUNDED", F.lit(1)).otherwise(F.lit(0))).alias("refunded_payments"),
        F.avg(F.col("gateway_latency_ms")).alias("avg_gateway_latency_ms"),
        F.avg(F.col("retry_count")).alias("avg_retry_count"),
        F.sum(F.col("transaction_amount")).alias("transaction_amount"),
        F.sum(F.col("effective_refund_amount")).alias("refund_amount"),
        F.sum(F.col("realized_revenue")).alias("net_collected_revenue"),
    )

    return aggregated.select(
        "payment_date",
        "gateway_name",
        "payment_method",
        "booking_channel",
        "city_segment",
        "payment_attempts",
        "successful_payments",
        "failed_payments",
        "pending_payments",
        "refunded_payments",
        F.round((F.col("successful_payments") / F.greatest(F.col("payment_attempts"), F.lit(1))) * 100, 2).alias("payment_success_rate_pct"),
        F.round("avg_gateway_latency_ms", 2).alias("avg_gateway_latency_ms"),
        F.round("avg_retry_count", 2).alias("avg_retry_count"),
        F.round("transaction_amount", 2).alias("transaction_amount"),
        F.round("refund_amount", 2).alias("refund_amount"),
        F.round("net_collected_revenue", 2).alias("net_collected_revenue"),
    )
