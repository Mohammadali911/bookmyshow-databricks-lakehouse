from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.silver_payments",
    comment="Conformed payment fact with gateway and settlement quality indicators.",
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
        "valid_payment_key": "payment_id IS NOT NULL",
        "valid_booking_reference": "booking_id IS NOT NULL",
        "valid_payment_ts": "payment_ts IS NOT NULL",
        "valid_transaction_amount": "transaction_amount >= 0",
    }
)
def silver_payments():
    source = (
        spark.read.table("workspace.bookmyshow_curated.bronze_payments")
        .withColumn("payment_ts_parsed", F.to_timestamp("payment_ts"))
        .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
    )

    latest_record = Window.partitionBy("payment_id").orderBy(
        F.col("updated_at_ts").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    return (
        source.withColumn("row_rank", F.row_number().over(latest_record))
        .filter(F.col("row_rank") == 1)
        .select(
            "payment_id",
            "booking_id",
            F.col("payment_ts_parsed").alias("payment_ts"),
            F.to_date("payment_ts_parsed").alias("payment_date"),
            "payment_method",
            "gateway_name",
            "payment_status",
            "payment_reference",
            F.col("transaction_amount").cast("double").alias("transaction_amount"),
            F.col("refund_amount").cast("double").alias("refund_amount"),
            "gateway_latency_ms",
            "retry_count",
            F.when(F.col("payment_status") == "SUCCESS", F.lit(1)).otherwise(F.lit(0)).alias("is_successful_payment"),
            F.when(F.col("payment_status") == "FAILED", F.lit(1)).otherwise(F.lit(0)).alias("is_failed_payment"),
            F.when(F.col("payment_status") == "REFUNDED", F.lit(1)).otherwise(F.lit(0)).alias("is_refunded_payment"),
            F.when(F.col("payment_status") == "SUCCESS", F.col("transaction_amount"))
            .when(F.col("payment_status") == "REFUNDED", F.col("transaction_amount") - F.col("refund_amount"))
            .otherwise(F.lit(0.0)).alias("net_collected_amount"),
            F.col("updated_at_ts").alias("updated_at"),
        )
    )
