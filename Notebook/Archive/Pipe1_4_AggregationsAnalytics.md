```python
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import avg, col, count, desc
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================
# START SPARK SESSION
# =========================================

# Configuration
project_id = "dejadsgl"
bq_dataset = "netflix"
temp_bucket = "netflix-group5-temp_gl"
gcs_data_bucket = "netflix_data_25"

# Spark configuration
sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("AggregationsAnalytics")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

# Create the Spark session
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector
spark.conf.set('temporaryGcsBucket', temp_bucket)

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

print("Spark session started.")

# =========================================
# LOAD TABLES
# =========================================

# Load data from BigQuery
df = spark.read \
            .format("bigquery") \
            .load(f"{project_id}.{bq_dataset}.unified_review_dataset")
print(f"\nLoaded table: unified_review_dataset")
df.printSchema()

# Zorg dat we date-velden hebben
df = (
    df
    .withColumn("day", F.to_date("review_date"))                # datum
    .withColumn("week_start", F.date_trunc("week", "day"))      # begin van de week
)
print("\nDONE: data loaded.")
# =============================================================
# DAILY TOP 10 MOVIES
# =============================================================
daily_views = (
    df
    .groupBy("day", "movie_id")
    .agg(
        F.count("*").alias("num_events"),          # aantal reviews / events
        F.avg("rating").alias("avg_rating")        # optioneel: gemiddelde rating
    )
)

w_day = Window.partitionBy("day").orderBy(F.desc("num_events"))

daily_top10_per_movie = (
    daily_views
    .withColumn("rank", F.row_number().over(w_day))
    .filter(F.col("rank") <= 10)
)

# =============================================================
# WEEKLY TOP 10 MOVIES
# =============================================================
weekly_views = (
    df
    .groupBy("week_start", "movie_id")
    .agg(
        F.count("*").alias("num_events"),
        F.avg("rating").alias("avg_rating")
    )
)

w_week = Window.partitionBy("week_start").orderBy(F.desc("num_events"))

weekly_top10_per_movie = (
    weekly_views
    .withColumn("rank", F.row_number().over(w_week))
    .filter(F.col("rank") <= 10)
)

weekly_top10_per_movie.show(10, truncate=False)

# =============================================================
# Recency per user
# =============================================================
user_recency = (
    df
    .groupBy("user_id")
    .agg(F.max("review_date").alias("last_interaction_date"))
    .withColumn(
        "days_since_last_interaction",
        F.datediff(F.current_date(), "last_interaction_date")
    )
)

user_recency.show(10, truncate=False)

# Activiteit laatste 30 dagen
last_30d = F.date_sub(F.current_date(), 30)

user_activity_30d = (
    df
    .filter(F.col("review_date") >= last_30d)
    .groupBy("user_id")
    .agg(
        F.count("*").alias("events_30d"),
        F.countDistinct("movie_id").alias("unique_titles_30d")
    )
)

user_activity_30d.show(10, truncate=False)

# =============================================================
# Basis-user set (alle unieke users uit df)
# =============================================================
user_base = (
    df
    .select("user_id")
    .distinct()
    .join(user_recency, on="user_id", how="left")
    .join(user_activity_30d, on="user_id", how="left")
    .fillna({
        "days_since_last_interaction": 9999,
        "events_30d": 0,
        "unique_titles_30d": 0
    })
)
user_base.show(10, truncate=False)

user_segments = (
    user_base
    .withColumn(
        "segment",
        F.when(F.col("events_30d") >= 20, "Power user")
         .when(F.col("days_since_last_interaction") <= 7, "Active")
         .when((F.col("days_since_last_interaction") > 7) & (F.col("days_since_last_interaction") <= 30), "At-risk")
         .otherwise("Dormant")
    )
)
user_segments.show(10, truncate=False)

# =============================================================
# Regional viewing patterns
# =============================================================

# regional_viewing_patterns = (
#     df
#     .groupBy("location_country", "genre_primary")
#     .agg(
#         F.count("*").alias("events"),
#         F.countDistinct("user_id").alias("unique_users"),
#         F.countDistinct("movie_id").alias("unique_titles")
#     )
# )

# w_region = Window.partitionBy("location_country")

# regional_viewing_patterns = (
#     regional_viewing_patterns
#     .withColumn(
#         "event_share_pct",
#         100 * F.col("events") / F.sum("events").over(w_region)
#     )
# )

# =============================================================
# Device usage statistics
# =============================================================

device_usage_stats = (
    df
    .groupBy("device_type")
    .agg(
        F.count("*").alias("events"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("movie_id").alias("unique_titles")
    )
)
device_usage_stats.show(10, truncate=False)

w_all_devices = Window.rowsBetween(Window.unboundedPreceding,
                                   Window.unboundedFollowing)

device_usage_stats = (
    device_usage_stats
    .withColumn(
        "event_share_pct",
        100 * F.col("events") / F.sum("events").over(w_all_devices)
    )
)
device_usage_stats.show(10, truncate=False)

# =============================================================
# Churn risk scores per user (0-100 scale)
# =============================================================

churn_risk_scores = (
    user_base
    # basisrisico: elke dag inactiviteit +2 punten, max 100
    .withColumn(
        "base_risk",
        F.least(F.col("days_since_last_interaction") * 2, F.lit(100))
    )
    # activiteitsschild: actieve users krijgen korting op risico
    .withColumn(
        "activity_bonus",
        F.least(F.col("events_30d") * 3, F.lit(40))  # max 40 punten korting
    )
    .withColumn(
        "churn_risk_score",
        F.when(F.col("base_risk") - F.col("activity_bonus") < 0, 0)
         .when(F.col("base_risk") - F.col("activity_bonus") > 100, 100)
         .otherwise(F.col("base_risk") - F.col("activity_bonus"))
    )
    .select(
        "user_id",
        "days_since_last_interaction",
        "events_30d",
        "unique_titles_30d",
        "churn_risk_score"
    )
)
churn_risk_scores.show(10, truncate=False)

print("\nDONE: Aggregations & Analytics.")
```

    Spark session started.
    
    Loaded table: unified_review_dataset
    root
     |-- review_id: string (nullable = false)
     |-- user_id: string (nullable = false)
     |-- movie_id: string (nullable = false)
     |-- review_date: string (nullable = false)
     |-- device_type: string (nullable = false)
     |-- is_verified_watch: string (nullable = false)
     |-- review_text: string (nullable = false)
     |-- sentiment: string (nullable = false)
     |-- sentiment_score: string (nullable = true)
     |-- rating: string (nullable = false)
     |-- rating_sentiment_score: double (nullable = true)
     |-- rating_sentiment_label: string (nullable = false)
     |-- helpful_votes: string (nullable = false)
     |-- total_votes: string (nullable = false)
    
    +-------------+----------+----------+-----------+-----------+-----------------+-------------------------------------------+---------+---------------+------+----------------------+----------------------+-------------+-----------+
    |review_id    |user_id   |movie_id  |review_date|device_type|is_verified_watch|review_text                                |sentiment|sentiment_score|rating|rating_sentiment_score|rating_sentiment_label|helpful_votes|total_votes|
    +-------------+----------+----------+-----------+-----------+-----------------+-------------------------------------------+---------+---------------+------+----------------------+----------------------+-------------+-----------+
    |review_000224|user_04104|movie_0015|2024-07-11 |Laptop     |True             |Not worth the time. Very predictable.      |negative |NULL           |1     |-1.0                  |negative              |2.0          |8.0        |
    |review_002341|user_06213|movie_0399|2025-12-17 |Mobile     |True             |Overhyped. Didn't live up to expectations. |negative |NULL           |1     |-1.0                  |negative              |2.0          |5.0        |
    |review_009316|user_00041|movie_0042|2024-02-17 |Mobile     |True             |Not worth the time. Very predictable.      |negative |NULL           |1     |-1.0                  |negative              |4.0          |4.0        |
    |review_004505|user_05669|movie_0306|2024-03-06 |Mobile     |True             |Disappointing. Expected much more.         |negative |NULL           |1     |-1.0                  |negative              |4.0          |4.0        |
    |review_010670|user_09275|movie_0985|2024-08-25 |Smart TV   |True             |Not worth the time. Very predictable.      |negative |NULL           |1     |-1.0                  |negative              |6.0          |6.0        |
    |review_003569|user_03421|movie_0254|2025-06-29 |Laptop     |True             |Disappointing. Expected much more.         |negative |NULL           |1     |-1.0                  |negative              |6.0          |10.0       |
    |review_014580|user_04870|movie_0503|2025-08-13 |Laptop     |True             |Not worth the time. Very predictable.      |negative |NULL           |1     |-1.0                  |negative              |4.0          |7.0        |
    |review_011988|user_08865|movie_0111|2025-09-19 |Mobile     |False            |Disappointing. Expected much more.         |negative |NULL           |1     |-1.0                  |negative              |3.0          |9.0        |
    |review_005075|user_00885|movie_0291|2024-11-17 |Mobile     |True             |Boring storyline, couldn't finish watching.|negative |NULL           |1     |-1.0                  |negative              |1.0          |6.0        |
    |review_002028|user_09083|movie_0544|2025-08-08 |Tablet     |True             |Not my cup of tea. Too violent.            |negative |NULL           |1     |-1.0                  |negative              |3.0          |5.0        |
    +-------------+----------+----------+-----------+-----------+-----------------+-------------------------------------------+---------+---------------+------+----------------------+----------------------+-------------+-----------+
    only showing top 10 rows
    
    
    DONE: data loaded.
    +-------------------+----------+----------+------------------+----+
    |week_start         |movie_id  |num_events|avg_rating        |rank|
    +-------------------+----------+----------+------------------+----+
    |2024-01-01 00:00:00|movie_0713|3         |2.6666666666666665|1   |
    |2024-01-01 00:00:00|movie_0886|3         |3.0               |2   |
    |2024-01-01 00:00:00|movie_1000|2         |4.0               |3   |
    |2024-01-01 00:00:00|movie_0764|2         |3.5               |4   |
    |2024-01-01 00:00:00|movie_0429|2         |3.0               |5   |
    |2024-01-01 00:00:00|movie_0778|2         |4.0               |6   |
    |2024-01-01 00:00:00|movie_0345|2         |3.5               |7   |
    |2024-01-01 00:00:00|movie_0599|2         |4.5               |8   |
    |2024-01-01 00:00:00|movie_0636|2         |3.5               |9   |
    |2024-01-01 00:00:00|movie_0816|2         |4.0               |10  |
    +-------------------+----------+----------+------------------+----+
    only showing top 10 rows
    
    +----------+---------------------+---------------------------+
    |user_id   |last_interaction_date|days_since_last_interaction|
    +----------+---------------------+---------------------------+
    |user_00001|2024-01-09           |675                        |
    |user_00002|2025-09-12           |63                         |
    |user_00003|2024-04-17           |576                        |
    |user_00004|2025-03-12           |247                        |
    |user_00005|2025-09-26           |49                         |
    |user_00006|2025-01-30           |288                        |
    |user_00007|2024-10-17           |393                        |
    |user_00010|2025-03-25           |234                        |
    |user_00011|2024-07-14           |488                        |
    |user_00013|2025-11-29           |-15                        |
    +----------+---------------------+---------------------------+
    only showing top 10 rows
    
    +----------+----------+-----------------+
    |user_id   |events_30d|unique_titles_30d|
    +----------+----------+-----------------+
    |user_00201|2         |2                |
    |user_03424|1         |1                |
    |user_04438|1         |1                |
    |user_03917|1         |1                |
    |user_04545|1         |1                |
    |user_03984|1         |1                |
    |user_06321|2         |2                |
    |user_03103|1         |1                |
    |user_01916|2         |2                |
    |user_02612|1         |1                |
    +----------+----------+-----------------+
    only showing top 10 rows
    
    +----------+---------------------+---------------------------+----------+-----------------+
    |user_id   |last_interaction_date|days_since_last_interaction|events_30d|unique_titles_30d|
    +----------+---------------------+---------------------------+----------+-----------------+
    |user_08690|2025-07-04           |133                        |0         |0                |
    |user_04103|2024-03-19           |605                        |0         |0                |
    |user_03762|2025-09-14           |61                         |0         |0                |
    |user_01411|2025-01-13           |305                        |0         |0                |
    |user_09487|2024-10-08           |402                        |0         |0                |
    |user_03544|2025-03-08           |251                        |0         |0                |
    |user_08494|2025-03-13           |246                        |0         |0                |
    |user_09937|2025-06-06           |161                        |0         |0                |
    |user_04989|2025-08-25           |81                         |0         |0                |
    |user_00201|2025-12-13           |-29                        |2         |2                |
    +----------+---------------------+---------------------------+----------+-----------------+
    only showing top 10 rows
    
    +----------+---------------------+---------------------------+----------+-----------------+-------+
    |user_id   |last_interaction_date|days_since_last_interaction|events_30d|unique_titles_30d|segment|
    +----------+---------------------+---------------------------+----------+-----------------+-------+
    |user_08690|2025-07-04           |133                        |0         |0                |Dormant|
    |user_04103|2024-03-19           |605                        |0         |0                |Dormant|
    |user_03762|2025-09-14           |61                         |0         |0                |Dormant|
    |user_01411|2025-01-13           |305                        |0         |0                |Dormant|
    |user_09487|2024-10-08           |402                        |0         |0                |Dormant|
    |user_03544|2025-03-08           |251                        |0         |0                |Dormant|
    |user_08494|2025-03-13           |246                        |0         |0                |Dormant|
    |user_09937|2025-06-06           |161                        |0         |0                |Dormant|
    |user_04989|2025-08-25           |81                         |0         |0                |Dormant|
    |user_00201|2025-12-13           |-29                        |2         |2                |Active |
    +----------+---------------------+---------------------------+----------+-----------------+-------+
    only showing top 10 rows
    
    +-----------+------+------------+-------------+
    |device_type|events|unique_users|unique_titles|
    +-----------+------+------------+-------------+
    |Laptop     |3786  |3103        |970          |
    |Mobile     |3932  |3144        |983          |
    |Tablet     |3800  |3106        |971          |
    |Smart TV   |3932  |3147        |969          |
    +-----------+------+------------+-------------+
    
    +-----------+------+------------+-------------+------------------+
    |device_type|events|unique_users|unique_titles|event_share_pct   |
    +-----------+------+------------+-------------+------------------+
    |Laptop     |3786  |3103        |970          |24.50485436893204 |
    |Mobile     |3932  |3144        |983          |25.449838187702266|
    |Tablet     |3800  |3106        |971          |24.59546925566343 |
    |Smart TV   |3932  |3147        |969          |25.449838187702266|
    +-----------+------+------------+-------------+------------------+
    
    +----------+---------------------------+----------+-----------------+----------------+
    |user_id   |days_since_last_interaction|events_30d|unique_titles_30d|churn_risk_score|
    +----------+---------------------------+----------+-----------------+----------------+
    |user_08690|133                        |0         |0                |100             |
    |user_04103|605                        |0         |0                |100             |
    |user_03762|61                         |0         |0                |100             |
    |user_01411|305                        |0         |0                |100             |
    |user_09487|402                        |0         |0                |100             |
    |user_03544|251                        |0         |0                |100             |
    |user_08494|246                        |0         |0                |100             |
    |user_09937|161                        |0         |0                |100             |
    |user_04989|81                         |0         |0                |100             |
    |user_00201|-29                        |2         |2                |0               |
    +----------+---------------------------+----------+-----------------+----------------+
    only showing top 10 rows
    
    
    DONE: Aggregations & Analytics.



```python
spark.stop()
```
