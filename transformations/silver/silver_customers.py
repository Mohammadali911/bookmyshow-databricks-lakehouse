from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="workspace.bookmyshow_curated.silver_customers",
    comment="Conformed customer dimension with loyalty, tenure, and demographic attributes.",
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
        "valid_customer_key": "customer_id IS NOT NULL",
        "valid_email_shape": "email LIKE '%@%'",
        "valid_phone_shape": "length(phone) >= 10",
        "valid_signup_ts": "signup_ts IS NOT NULL",
    }
)
def silver_customers():
    source = (
        spark.read.table("workspace.bookmyshow_curated.bronze_customers")
        .withColumn("signup_ts_parsed", F.to_timestamp("signup_ts"))
        .withColumn("updated_at_ts", F.to_timestamp("updated_at"))
    )

    latest_record = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at_ts").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    return (
        source.withColumn("row_rank", F.row_number().over(latest_record))
        .filter(F.col("row_rank") == 1)
        .select(
            "customer_id",
            "full_name",
            F.lower("email").alias("email"),
            "phone",
            "city",
            "state",
            "loyalty_tier",
            "marketing_opt_in",
            F.col("signup_ts_parsed").alias("signup_ts"),
            "birth_year",
            (F.year(F.col("updated_at_ts")) - F.col("birth_year")).alias("customer_age"),
            F.when((F.year(F.col("updated_at_ts")) - F.col("birth_year")) < 25, F.lit("18_to_24"))
            .when((F.year(F.col("updated_at_ts")) - F.col("birth_year")) < 35, F.lit("25_to_34"))
            .when((F.year(F.col("updated_at_ts")) - F.col("birth_year")) < 45, F.lit("35_to_44"))
            .otherwise(F.lit("45_plus")).alias("age_band"),
            F.datediff(F.to_date("updated_at_ts"), F.to_date("signup_ts_parsed")).alias("tenure_days"),
            F.col("updated_at_ts").alias("updated_at"),
        )
    )
