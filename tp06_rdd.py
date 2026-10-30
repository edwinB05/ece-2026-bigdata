"""Lab 06 - Introduction to Spark's RDD API (run: spark-submit --master 'local[*]' tp06_rdd.py)"""
import re, time
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# RDD creation and partitions
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users partitions :", users_lines.getNumPartitions())
print("orders partitions:", orders_lines.getNumPartitions())
orders_repartitioned = orders_lines.repartition(8)
print("repartition(8)   :", orders_repartitioned.getNumPartitions())
print("coalesce(2)      :", orders_repartitioned.coalesce(2).getNumPartitions())

# Lazy evaluation and lineage
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print("count 1:", orders_data.count())
print("count 2:", orders_data.count())

# The cost of no schema: the multiline address
for line in users_lines.take(6):
    print(repr(line))
print("lignes lues par textFile:", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("vrais utilisateurs:", users_records.count())

# Manual parsing of orders
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {"order_id": order_id, "user_id": user_id, "date": date,
            "quantity": int(quantity), "product": product.strip().lower()}

orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)

# Key-value RDDs: reduceByKey vs groupByKey
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
user_quantity_grouped = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)
print("reduceByKey:", user_quantity.take(3))
print("groupByKey :", user_quantity_grouped.take(3))
print("meme resultat:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

# Manual deduplication
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("commandes:", orders_rdd.count(), "| apres dedup:", orders_dedup.count())

# Joining orders with users
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
for row in enriched.take(2):
    print(row)
print("lignes join:", enriched.count())
print("lignes leftOuterJoin:", orders_kv.leftOuterJoin(users_kv).count())

# Exercise 1: aggregateByKey -> (order_count, total_quantity), then average
agg = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0), lambda acc, q: (acc[0] + 1, acc[1] + q), lambda a, b: (a[0] + b[0], a[1] + b[1]))
print("ex1 (nb commandes, total):", agg.take(3))
print("ex1 moyenne:", agg.mapValues(lambda t: round(t[1] / t[0], 2)).take(3))

# Exercise 2: fake user_id + leftOuterJoin
fake = sc.parallelize([{"order_id": "fake-order", "user_id": "fake-user",
                        "date": "2020-01-01 00:00:00", "quantity": 1, "product": "bread"}])
left = orders_rdd.union(fake).map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
orphans = left.filter(lambda kv: kv[1][1] is None)
print("ex2 commandes sans utilisateur:", orphans.count())

# Exercise 3: DataFrame version
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
df_quantity = orders_df.groupBy("user_uuid").sum("quantity")
df_quantity.explain()
print(df_quantity.take(3))
print(user_quantity.toDebugString().decode())

# Exercise 4: timing with and without cache
orders_nocache = orders_data.map(parse_order)
uq_nocache = orders_nocache.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("stockage avec cache:", orders_rdd.getStorageLevel())
print("stockage sans cache:", orders_nocache.getStorageLevel())
for label, rdd in [("avec cache  orders", orders_rdd), ("avec cache  user_quantity", user_quantity),
                   ("sans cache  orders", orders_nocache), ("sans cache  user_quantity", uq_nocache)]:
    t = time.perf_counter()
    n = rdd.count()
    print(f"{label:<28} {time.perf_counter() - t:.3f}s  ({n})")

spark.stop()
