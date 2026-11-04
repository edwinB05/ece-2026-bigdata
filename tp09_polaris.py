"""Lab 09 - Govern Iceberg tables with Polaris (lab-2), with exercises 1 and 2.

Prerequisites: Polaris deployed (lab-1), lab.silver.* tables of lab 07, ~/.polaris-credentials.env,
and ./tp09_silver_reader.sh run once for exercise 2.
Run:
  eval "$(aws configure export-credentials --profile default --format env)"
  export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181" \
         POLARIS_CATALOG="$KUBERNETES_NAMESPACE" AWS_REGION="us-east-1"
  set -a; . /home/onyxia/.polaris-credentials.env; set +a
  spark-submit --master 'local[*]' \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem tp09_polaris.py
"""
import os
from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
os.environ.setdefault("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
spark = None


def section(title):
    print("\n" + "=" * 10, title, "=" * 10)


def make_spark(principal):
    """(Re)create the Spark session with the credential of a Polaris principal."""
    global spark
    if spark is not None:
        spark.stop()
    cred = os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    spark = (
        SparkSession.builder.appName("lab-polaris").master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Hadoop catalog of the Spark lab
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
        # Polaris REST catalog
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
        .config("spark.sql.catalog.polaris.credential", cred)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("Polaris principal:", principal)


def attempt(label, sql):
    """Run a statement and report whether Polaris allowed it."""
    try:
        spark.sql(sql).collect()
        print(f"[{label}] ALLOWED")
    except Exception as e:
        msg = str(e)
        print(f"[{label}] DENIED:", msg[msg.find("Forbidden"):][:200] if "Forbidden" in msg else msg[:200])


def exists(name):
    try:
        spark.table(name)
        return True
    except Exception:
        return False


def s3_count(prefix):
    out = os.popen(f'aws s3 --profile default ls "s3://{BUCKET}/{prefix}" --recursive').read()
    return len([line for line in out.splitlines() if line.strip()])


# ---------------------------------------------------------------- Session and catalogs (dev)
section("Session as dev")
make_spark("dev")
if not exists("lab.silver.orders"):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet").writeTo("lab.silver.users").createOrReplace()
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet").writeTo("lab.silver.orders").createOrReplace()
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW NAMESPACES IN polaris").show()

# ---------------------------------------------------------------- Register silver.orders
section("Register the silver table in Polaris")
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
if not exists("polaris.silver.orders"):
    version = os.popen(f'aws s3 --profile default cp '
                       f'"s3://{BUCKET}/warehouse/silver/orders/metadata/version-hint.text" -').read().strip()
    before = s3_count("warehouse/silver/orders/")
    spark.sql(f"""CALL polaris.system.register_table(
        table => 'silver.orders',
        metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json')""").show(truncate=False)
    print("objects under warehouse/silver/orders/ before:", before, "| after:", s3_count("warehouse/silver/orders/"))
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()

# ---------------------------------------------------------------- Create tables as dev
section("Create tables as dev")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
os.system(f"aws s3 --profile default ls \"s3://{BUCKET}/warehouse/silver/\" --recursive | awk '{{print $4}}'")

# ---------------------------------------------------------------- Schema change as dev
section("Schema change as dev")
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show()
snaps_before = spark.sql("SELECT count(*) AS n FROM polaris.silver.orders.snapshots").collect()[0]["n"]

# ---------------------------------------------------------------- Read as the analyst
section("Read as the analyst")
make_spark("analyst")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
attempt("INSERT orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
attempt("ALTER users", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

make_spark("dev")
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
print("snapshots before:", snaps_before, "| after the denied INSERT:",
      spark.sql("SELECT count(*) AS n FROM polaris.silver.orders.snapshots").collect()[0]["n"])

# ---------------------------------------------------------------- Exercise 1
section("Exercise 1: drop the country column (metadata only)")
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
spark.sql("DESCRIBE polaris.silver.users").show()
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots").show(truncate=False)
spark.sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries ORDER BY timestamp").show(truncate=False)

# ---------------------------------------------------------------- Exercise 2
section("Exercise 2: silver_reader limited to the silver namespace")
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
spark.sql("SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product") \
    .writeTo("polaris.gold.orders_by_product").createOrReplace()
if "POLARIS_SILVER_TESTER_CREDENTIAL" in os.environ:
    make_spark("silver_tester")
    attempt("read silver.orders", "SELECT count(*) FROM polaris.silver.orders")
    attempt("list silver", "SHOW TABLES IN polaris.silver")
    attempt("read gold.orders_by_product", "SELECT * FROM polaris.gold.orders_by_product")
    attempt("list gold", "SHOW TABLES IN polaris.gold")
    attempt("write silver.orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    make_spark("dev")
else:
    print("POLARIS_SILVER_TESTER_CREDENTIAL missing: run ./tp09_silver_reader.sh, then reload the env file")

# ---------------------------------------------------------------- Cleanup (silver.orders stays registered)
section("Cleanup")
spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.stop()
