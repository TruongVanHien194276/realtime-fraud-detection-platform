from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def build_spark(app_name: str, warehouse: Path) -> SparkSession:
    warehouse.mkdir(parents=True, exist_ok=True)
    Path("data/tmp/spark").mkdir(parents=True, exist_ok=True)

    session = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", warehouse.resolve().as_uri())
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.jsonGenerator.ignoreNullFields", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.files.maxPartitionBytes", "67108864")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session


def insert_new_transactions(
    spark: SparkSession,
    frame: DataFrame,
    table_name: str,
    view_name: str,
) -> None:
    namespace = table_name.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    if not spark.catalog.tableExists(table_name):
        # Chỉ tạo schema và partition spec; chưa ghi dữ liệu.
        (frame.limit(0).writeTo(table_name).using("iceberg").partitionedBy("event_date").create())

    frame.createOrReplaceTempView(view_name)

    # Insert-only MERGE: transaction đã có sẽ giữ nguyên, kể cả loaded_at và batch_id.
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON target.transaction_id = source.transaction_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
