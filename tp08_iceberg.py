"""Lab 08 - Explore Iceberg tables with Spark (snapshots, time travel, schema evolution).

Requires the lab.silver.* Iceberg tables of lab 07 (recreated from the silver Parquet if missing).
Run:
  export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
  spark-submit --master 'local[*]' \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 tp08_iceberg.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def section(title):
    print("\n" + "=" * 10, title, "=" * 10)


def table_exists(name):
    try:
        spark.table(name)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------- SparkSession
spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SHOW NAMESPACES IN lab").show()

# ---------------------------------------------------------------- Create the table if needed
section("Create an Iceberg table (if missing)")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
if not table_exists("lab.silver.orders"):
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet").writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

# ---------------------------------------------------------------- Inspect files and metadata
section("Inspect the files")
os.system(f'aws s3 ls "s3://{BUCKET}/warehouse/silver/orders/" --recursive')
section("Explore Iceberg metadata")
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes "
          "FROM lab.silver.orders.files").show(truncate=False)

# ---------------------------------------------------------------- Second snapshot
section("Create a second snapshot")
stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots "
          "ORDER BY committed_at").show(truncate=False)
spark.sql("SELECT count(*) FROM lab.silver.orders").show()

# ---------------------------------------------------------------- Time travel
section("Time travel")
snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
print(snapshots)
first_snapshot = snapshots[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS first_snapshot FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()
spark.sql("SELECT count(*) AS current FROM lab.silver.orders").show()

# ---------------------------------------------------------------- Schema evolution
section("Schema evolution")
if "source" not in spark.table("lab.silver.orders").columns:
    spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
spark.sql("SELECT source, count(*) FROM lab.silver.orders GROUP BY source").show()

# ---------------------------------------------------------------- Compare with Parquet
section("Compare Iceberg with Parquet")
os.system(f'aws s3 ls "s3://{BUCKET}/silver/spark/orders.parquet/" --recursive')
os.system(f'aws s3 ls "s3://{BUCKET}/warehouse/silver/orders/metadata/"')

# ---------------------------------------------------------------- Exercises
section("Exercise 1: lab.silver.users as an Iceberg table")
if not table_exists("lab.silver.users"):
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet").writeTo("lab.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM lab.silver.users").show()

section("Exercise 2: append a new batch of users")
base_cols = ["user_id", "username", "name", "sex", "email", "birthdate", "street", "city", "state", "zip_code"]
new_users = (spark.table("lab.silver.users").select(*base_cols).limit(2)
             .withColumn("user_id", F.expr("uuid()")))
if "country" in spark.table("lab.silver.users").columns:
    new_users = new_users.withColumn("country", F.lit(None).cast("string"))
new_users.writeTo("lab.silver.users").append()
users_snaps = spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.users.snapshots "
                        "ORDER BY committed_at")
users_snaps.show(truncate=False)

section("Exercise 3: time travel before / after the append")
first_users_snap = users_snaps.collect()[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS before_append FROM lab.silver.users VERSION AS OF {first_users_snap}").show()
spark.sql("SELECT count(*) AS after_append FROM lab.silver.users").show()

section("Exercise 4: add a country column")
if "country" not in spark.table("lab.silver.users").columns:
    spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.sql("""
    INSERT INTO lab.silver.users
    SELECT uuid(), username, name, sex, email, birthdate, street, city, state, zip_code, 'US'
    FROM lab.silver.users LIMIT 1
""")
spark.sql("SELECT country, count(*) FROM lab.silver.users GROUP BY country").show()
spark.sql("DESCRIBE lab.silver.users").show()

section("Exercise 5: S3 layout of users vs orders")
os.system(f'aws s3 ls "s3://{BUCKET}/warehouse/silver/users/" --recursive')
os.system(f'aws s3 ls "s3://{BUCKET}/warehouse/silver/orders/" --recursive')

section("Exercise 6: Parquet vs Iceberg vs Iceberg catalog")
print("""- Parquet: a columnar FILE format. It stores data but has no notion of table,
  version or transaction.
- Iceberg: a TABLE format. It organises Parquet files with metadata (schema, partitions,
  snapshots, manifests): atomic commits, time travel, schema evolution.
- Iceberg catalog: the service that records where the current version of each table is,
  so several engines and users can share tables safely (atomic commits, access control).""")

spark.stop()
