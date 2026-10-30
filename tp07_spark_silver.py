"""Lab 07 - Bronze to silver with Spark DataFrames, Spark SQL, Parquet and Iceberg.

Run:
  export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
  spark-submit --master 'local[*]' \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 tp07_spark_silver.py
"""
import os
import re
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def section(title):
    print("\n" + "=" * 10, title, "=" * 10)


# ---------------------------------------------------------------- SparkSession
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(spark, spark.version, BUCKET)

# ---------------------------------------------------------------- Bronze layer
section("Bronze layer")
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True,
                              inferSchema=True, samplingRatio=0.01, multiLine=True)
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", header=True,
                               inferSchema=True, samplingRatio=0.01, multiLine=True)
users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)

# ---------------------------------------------------------------- Silver users
section("Silver users (DataFrame API)")
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
# regexp_extract() returns "" (not NULL) when there is no match
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

# ---------------------------------------------------------------- Silver orders
section("Silver orders (window deduplication)")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
orders_with_duplicates = orders_typed.unionByName(orders_typed.limit(3))
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

# ---------------------------------------------------------------- Spark SQL
section("Same transformation with Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("asserts OK")
stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state, count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users GROUP BY state ORDER BY users DESC
""").show(10)

# ---------------------------------------------------------------- Parquet
section("Write the silver layer as Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
os.system(f'aws s3 ls "s3://{BUCKET}/silver/" --recursive')
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("parquet asserts OK")

# ---------------------------------------------------------------- Iceberg
section("Write the silver layer as Iceberg tables")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
os.system(f'aws s3 ls "s3://{BUCKET}/warehouse/" --recursive')
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes "
          "FROM lab.silver.orders.files").show(truncate=False)
assert spark.table("lab.silver.users").count() == users_bronze.count()
assert spark.table("lab.silver.orders").count() == stg_orders.count()
print("iceberg asserts OK")

# ---------------------------------------------------------------- Compare with dbt
section("Compare with the dbt lab (DuckDB)")
os.system(f"""duckdb -c "CREATE SECRET (TYPE s3, PROVIDER credential_chain, ENDPOINT '{os.environ['AWS_S3_ENDPOINT']}', REGION 'us-east-1', URL_STYLE 'path');
SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity FROM 's3://{BUCKET}/silver/dbt/orders.parquet'
UNION ALL SELECT 'spark', count(*), sum(quantity) FROM 's3://{BUCKET}/silver/spark/orders.parquet/*.parquet';
SELECT 'dbt' AS engine, count(*) AS users FROM 's3://{BUCKET}/silver/dbt/users.parquet'
UNION ALL SELECT 'spark', count(*) FROM 's3://{BUCKET}/silver/spark/users.parquet/*.parquet';
DESCRIBE SELECT * FROM 's3://{BUCKET}/silver/dbt/orders.parquet';
DESCRIBE SELECT * FROM 's3://{BUCKET}/silver/spark/orders.parquet/*.parquet';" """)

# ---------------------------------------------------------------- Exercise 1
section("Exercise 1: birthdate_is_valid (DataFrame vs SQL)")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn("birthdate_is_valid",
                ~F.coalesce(F.col("birthdate") > F.to_date("first_order_at"), F.lit(False)))
    .drop("first_order_at")
)
stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql("""
    SELECT u.*, NOT coalesce(u.birthdate > to_date(f.first_order_at), false) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id) f
      ON u.user_id = f.user_id
""")
diff = (users_valid_df.select("user_id", "birthdate_is_valid")
        .exceptAll(users_valid_sql.select("user_id", "birthdate_is_valid")))
print("differences DataFrame vs SQL:", diff.count())
users_valid_df.groupBy("birthdate_is_valid").count().show()

# ---------------------------------------------------------------- Exercise 2
section("Exercise 2: Python UDF vs pandas_udf vs regexp_extract")
ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")


@F.udf("string")
def zip_udf(address):
    m = ZIP_RE.search(address or "")
    return m.group(2) if m else None


def bench(label, column):
    t = time.perf_counter()
    for _ in range(3):
        users_bronze.select(column.alias("zip")).write.format("noop").mode("overwrite").save()
    print(f"{label:<16} {(time.perf_counter() - t) / 3:.3f}s per run")


bench("regexp_extract", F.regexp_extract("address", "([A-Z]{2}) ([0-9]{5})$", 2))
bench("python udf", zip_udf("address"))
try:
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("string")
    def zip_pudf(s: pd.Series) -> pd.Series:
        return s.str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1]

    bench("pandas_udf", zip_pudf("address"))
except Exception as e:
    print("pandas_udf unavailable:", type(e).__name__, e)

# ---------------------------------------------------------------- Exercise 3
section("Exercise 3: cache the bronze DataFrames")


def timed(label, f):
    t = time.perf_counter()
    r = f()
    print(f"{label:<24} {time.perf_counter() - t:.3f}s -> {r}")


timed("no cache users", users_bronze.count)
users_bronze.cache()
orders_bronze.cache()
timed("1st count (fills)", users_bronze.count)
timed("2nd count (cached)", users_bronze.count)
timed("orders 1st count", orders_bronze.count)
timed("orders 2nd count", orders_bronze.count)

# ---------------------------------------------------------------- Exercise 4
section("Exercise 4: append + time travel, Iceberg vs Parquet")
new_order = stg_orders.limit(1).withColumn("order_id", F.expr("uuid()"))
new_order.writeTo("lab.silver.orders").append()
snaps = spark.sql("SELECT snapshot_id, operation, committed_at "
                  "FROM lab.silver.orders.snapshots ORDER BY committed_at")
snaps.show(truncate=False)
first_snap = snaps.collect()[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS before_append FROM lab.silver.orders VERSION AS OF {first_snap}").show()
spark.sql("SELECT count(*) AS now FROM lab.silver.orders").show()
pq = f"s3a://{BUCKET}/silver/spark/orders_ex4.parquet"
stg_orders.write.mode("overwrite").parquet(pq)
new_order.write.mode("append").parquet(pq)
print("parquet after append:", spark.read.parquet(pq).count(), "(no way to read the previous state)")

spark.stop()
