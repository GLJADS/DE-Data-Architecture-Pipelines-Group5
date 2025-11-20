#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, avg, month, year, to_date, datediff, lit, first, max as spark_max, min as spark_min, countDistinct

# -------------------------------------------------------------------
# 1. Configuration
# -------------------------------------------------------------------
project_id = "de2025-471807"
bq_dataset_processed = "netflix_processed"  # Dataset for cleaned/processed data and aggregations
temp_bucket = "netflix-group5-temp"
gcs_bucket = "netflix_data_25"  # GCS bucket for raw data
processed_path = "/home/jovyan/data/processed/"  # Optional: also save to local CSV

# -------------------------------------------------------------------
# 2. Spark session setup with GCS and BigQuery support
# -------------------------------------------------------------------
sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("BatchPipelineNetflix")  # Changed from DataQualityCheck
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data
spark.conf.set('temporaryGcsBucket', temp_bucket)

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

print("✅ Spark session created with GCS")
print(f"   App Name: BatchPipelineNetflix")
print(f"   GCS Bucket: {gcs_bucket} (reading raw data)")
print(f"   Processed Dataset: {bq_dataset_processed} (writing cleaned tables + aggregations)")


# In[5]:


# -------------------------------------------------------------------
# 3. Load all tables from Google Cloud Storage
# -------------------------------------------------------------------
# Map table names to CSV files in GCS
tables = {
    "users": "users.csv",
    "movies": "movies.csv",
    "watch_history": "watch_history.csv",
    "recommendation_logs": "recommendation_logs.csv",
    "reviews": "reviews.csv",
    "search_logs": "search_logs.csv"
}

dataframes = {}
for name, csv_file in tables.items():
    gcs_path = f"gs://{gcs_bucket}/{csv_file}"
    print(f"Loading {name} from: {gcs_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(gcs_path)
    dataframes[name] = df
    print(f"✅ Loaded {name}: {df.count()} rows, {len(df.columns)} columns")

print("\n" + "="*80)
print("📊 DATA LOADING COMPLETE (from GCS)")
print("="*80)


# In[6]:


# -------------------------------------------------------------------
# 4. Inspect Schemas and Key Relationships
# -------------------------------------------------------------------
print("\n" + "="*80)
print("🔍 SCHEMA INSPECTION")
print("="*80)

for name, df in dataframes.items():
    print(f"\n📋 {name.upper()} Schema:")
    print("-" * 80)
    df.printSchema()
    
    # Check for key columns that will be used for joins
    key_columns = []
    if "user_id" in df.columns:
        key_columns.append("user_id")
    if "movie_id" in df.columns:
        key_columns.append("movie_id")
    if "session_id" in df.columns:
        key_columns.append("session_id")
    
    if key_columns:
        print(f"\n   🔑 Key columns for joins: {', '.join(key_columns)}")
        
        # Sample a few rows to understand data structure
        print(f"\n   📝 Sample data (first 3 rows):")
        df.select(key_columns).show(3, truncate=False)

print("\n" + "="*80)
print("✅ Schema inspection complete")
print("="*80)


# In[7]:


# -------------------------------------------------------------------
# 5. Check missing values and duplicates
# -------------------------------------------------------------------
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType, DecimalType

def check_data_quality(df, name):
    print(f"\n📊 Data Quality Report: {name}")
    total_rows = df.count()
    print(f"   Total rows: {total_rows}")
    print(f"   Total columns: {len(df.columns)}")
    
    # Missing values per column
    missing_counts = {}
    for col_name in df.columns:
        col_type = dict(df.dtypes)[col_name]
        col_expr = col(col_name)
        
        # Check if column is numeric (can use isnan)
        is_numeric = col_type in ['double', 'float', 'int', 'bigint', 'decimal']
        
        if is_numeric:
            # For numeric columns, check both null and nan
            missing = df.filter(col_expr.isNull() | isnan(col_expr)).count()
        else:
            # For non-numeric columns, only check null
            missing = df.filter(col_expr.isNull()).count()
        
        if missing > 0:
            missing_counts[col_name] = missing
    
    if missing_counts:
        print(f"   ⚠️  Missing values found:")
        for col_name, count in missing_counts.items():
            pct = (count / total_rows) * 100
            print(f"      - {col_name}: {count} ({pct:.1f}%)")
    else:
        print(f"   ✅ No missing values")
    
    # Duplicates
    duplicate_count = total_rows - df.dropDuplicates().count()
    if duplicate_count > 0:
        pct = (duplicate_count / total_rows) * 100
        print(f"   ⚠️  Duplicates: {duplicate_count} rows ({pct:.1f}%)")
    else:
        print(f"   ✅ No duplicates")
    
    return missing_counts, duplicate_count

# Check all dataframes
quality_reports = {}
for name, df in dataframes.items():
    missing, duplicates = check_data_quality(df, name)
    quality_reports[name] = {"missing": missing, "duplicates": duplicates}


# In[8]:


# -------------------------------------------------------------------
# 6. Clean data: Remove missing values, empty columns, and duplicates
# -------------------------------------------------------------------
print("\n" + "="*80)
print("🧹 DATA CLEANING")
print("="*80)
def clean_dataframe(df, critical_columns=None):
    """
    Clean dataframe by removing:
    - Columns that are entirely null
    - Rows with missing values in critical columns (or all columns if not specified)
    - Duplicate rows
    """
    # Remove columns that are entirely null
    total_rows = df.count()
    columns_to_keep = []
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count < total_rows:  # Keep column if it has at least one non-null value
            columns_to_keep.append(col_name)
    
    df_clean = df.select(columns_to_keep)
    
    # Remove rows with missing values
    # If critical_columns specified, only check those; otherwise check all columns
    if critical_columns:
        # Only remove rows where critical columns are missing
        condition = None
        for col_name in critical_columns:
            if col_name in df_clean.columns:
                col_expr = col(col_name)
                col_type = dict(df_clean.dtypes)[col_name]
                is_numeric = col_type in ['double', 'float', 'int', 'bigint', 'decimal']
                
                if is_numeric:
                    col_condition = col_expr.isNull() | isnan(col_expr)
                else:
                    col_condition = col_expr.isNull()
                
                if condition is None:
                    condition = col_condition
                else:
                    condition = condition | col_condition
        
        if condition is not None:
            df_clean = df_clean.filter(~condition)
    else:
        # Remove rows with any missing values (original behavior)
        df_clean = df_clean.dropna()
    
    # Remove duplicate rows
    df_clean = df_clean.dropDuplicates()
    
    return df_clean

# Define critical columns for each table (columns that must not be null)
critical_columns_map = {
    "users": ["user_id", "email"],  # User must have ID and email
    "movies": ["movie_id", "title"],  # Movie must have ID and title
    "watch_history": ["session_id", "user_id", "movie_id"],  # Watch session must have these
    "recommendation_logs": ["user_id", "movie_id"],  # Recommendation must have user and movie
    "reviews": ["user_id", "movie_id"],  # Review must have user and movie
    "search_logs": ["user_id"]  # Search must have user
}

cleaned_dataframes = {}
for name, df in dataframes.items():
    original_count = df.count()
    original_cols = len(df.columns)
    critical_cols = critical_columns_map.get(name, None)
    df_clean = clean_dataframe(df, critical_columns=critical_cols)
    cleaned_count = df_clean.count()
    cleaned_cols = len(df_clean.columns)
    cleaned_dataframes[name] = df_clean
    
    removed_rows = original_count - cleaned_count
    removed_cols = original_cols - cleaned_cols
    print(f"✅ {name}: {original_count} → {cleaned_count} rows, {original_cols} → {cleaned_cols} cols (removed {removed_rows} rows, {removed_cols} cols)")


# In[9]:


# -------------------------------------------------------------------
# 7. Save cleaned data to BigQuery
# -------------------------------------------------------------------
print("\n" + "="*80)
print("📤 SAVING CLEANED DATA TO BIGQUERY")
print("="*80)
print("\nWriting cleaned data to BigQuery...")

# Map table names for BigQuery (use same names as raw, or add suffix)
table_name_map = {
    "users": "Users",
    "movies": "Movies",
    "watch_history": "WatchHistory",
    "recommendation_logs": "RecommendationLogs",
    "reviews": "Reviews",
    "search_logs": "SearchLogs"
}

for name, df_clean in cleaned_dataframes.items():
    table_name = table_name_map.get(name, name.capitalize())
    bq_table = f"{project_id}.{bq_dataset_processed}.{table_name}"
    
    print(f"   Writing {name} to {bq_table}...")
    df_clean.write.format('bigquery') \
        .option('table', bq_table) \
        .mode("overwrite") \
        .save()
    print(f"   ✅ {name} written successfully ({df_clean.count()} rows)")

print(f"\n✅ All cleaned data written to BigQuery dataset: {bq_dataset_processed}")
print("\n🎉 Data quality check and cleaning completed!")


# In[10]:


# -------------------------------------------------------------------
# 8. Join Tables (Star Schema)
# -------------------------------------------------------------------
print("\n" + "="*80)
print("🔗 JOINING TABLES (STAR SCHEMA)")
print("="*80)
print("\nBuilding star schema: watch_history as fact table, others as dimensions...")

# Start with the fact table (watch_history)
fact_table = cleaned_dataframes["watch_history"]
print(f"\n✅ Fact table (watch_history): {fact_table.count()} rows")

# Join with dimension tables
# 1. Join with users (dimension)
joined_df = fact_table.join(
    cleaned_dataframes["users"],
    on="user_id",
    how="inner"
)
print(f"✅ After joining with users: {joined_df.count()} rows")

# 2. Join with movies (dimension)
joined_df = joined_df.join(
    cleaned_dataframes["movies"],
    on="movie_id",
    how="inner"
)
print(f"✅ After joining with movies: {joined_df.count()} rows")

# 3. Optionally join with reviews (for rating information)
# Use left join to keep all watch history even if no review exists
# Rename reviews.rating to user_rating to avoid conflict with movies.rating (content rating)
# Aggregate reviews: if user has multiple reviews for same movie, take average rating
reviews_for_join = cleaned_dataframes["reviews"].groupBy(
    "user_id", 
    "movie_id"
).agg(
    avg("rating").alias("user_rating")
)

# Drop user_rating if it already exists (to avoid ambiguity)
if "user_rating" in joined_df.columns:
    joined_df = joined_df.drop("user_rating")

joined_df = joined_df.join(
    reviews_for_join,
    on=["user_id", "movie_id"],
    how="left"
)
print(f"✅ After joining with reviews: {joined_df.count()} rows")

# Show sample of joined data
print("\n📊 Sample of joined data (first 5 rows):")
print("-" * 80)
joined_df.select(
    "session_id", "user_id", "movie_id", "watch_date",
    "country", "subscription_plan", "title", "genre_primary",
    "watch_duration_minutes", "action"
).show(5, truncate=False)

print("\n✅ Star schema join complete!")
print(f"   Final joined dataset: {joined_df.count()} rows, {len(joined_df.columns)} columns")


# In[11]:


# -------------------------------------------------------------------
# 9. Transform Data (Parse Timestamps, Prepare for Aggregations)
# -------------------------------------------------------------------
print("\n" + "="*80)
print("🔄 DATA TRANSFORMATION")
print("="*80)

from pyspark.sql.functions import to_timestamp

# Parse watch_date to timestamp if it's not already
# Check the current type
print("\n📅 Checking date column types...")
print(f"   watch_date type: {dict(joined_df.dtypes).get('watch_date', 'N/A')}")

# Convert watch_date to timestamp if it's a string
if 'watch_date' in joined_df.columns:
    # Try to parse as timestamp
    joined_df = joined_df.withColumn(
        "watch_date_parsed",
        to_timestamp(col("watch_date"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Extract year and month for monthly aggregations
    joined_df = joined_df.withColumn("watch_year", year(col("watch_date_parsed")))
    joined_df = joined_df.withColumn("watch_month", month(col("watch_date_parsed")))
    
    print("✅ Parsed watch_date and extracted year/month")

# Handle missing watch_duration_minutes (fill with 0 or median)
# For now, we'll filter out nulls in aggregations, but we could also fill
print(f"\n📊 Data quality after transformation:")
print(f"   Total rows: {joined_df.count()}")
print(f"   Rows with watch_duration_minutes: {joined_df.filter(col('watch_duration_minutes').isNotNull()).count()}")
print(f"   Rows with watch_date_parsed: {joined_df.filter(col('watch_date_parsed').isNotNull()).count()}")

print("\n✅ Data transformation complete!")


# In[12]:


# -------------------------------------------------------------------
# 10. Content Performance Aggregations
# -------------------------------------------------------------------
print("\n" + "="*80)
print("📈 CONTENT PERFORMANCE AGGREGATIONS")
print("="*80)

# Filter out null values for aggregations
df_for_agg = joined_df.filter(
    col("watch_date_parsed").isNotNull() &
    col("watch_duration_minutes").isNotNull()
)

# 1. Average rating per genre (monthly)
print("\n1️⃣ Computing average rating per genre (monthly)...")
content_performance = df_for_agg.filter(col("user_rating").isNotNull()).groupBy(
    "watch_year",
    "watch_month",
    "genre_primary"
).agg(
    avg("user_rating").alias("avg_rating"),
    count("*").alias("total_views"),
    spark_sum("watch_duration_minutes").alias("total_watch_time_minutes"),
    countDistinct("movie_id").alias("unique_movies"),
    countDistinct("user_id").alias("unique_users")
).orderBy("watch_year", "watch_month", "genre_primary")

print("✅ Content performance aggregation complete")
print(f"   Rows in content_performance: {content_performance.count()}")
print("\n📊 Sample content performance data:")
content_performance.show(10, truncate=False)

# 2. Genre performance over time (overall, not just monthly)
print("\n2️⃣ Computing overall genre performance...")
genre_performance = df_for_agg.groupBy("genre_primary").agg(
    count("*").alias("total_views"),
    spark_sum("watch_duration_minutes").alias("total_watch_time_minutes"),
    avg("watch_duration_minutes").alias("avg_watch_duration"),
    countDistinct("movie_id").alias("unique_movies"),
    countDistinct("user_id").alias("unique_users"),
    avg("user_rating").alias("avg_rating")
).orderBy(spark_sum("watch_duration_minutes").desc())

print("✅ Genre performance aggregation complete")
print(f"   Rows in genre_performance: {genre_performance.count()}")
print("\n📊 Top genres by watch time:")
genre_performance.show(10, truncate=False)


# In[13]:


# -------------------------------------------------------------------
# 11. User Engagement Aggregations
# -------------------------------------------------------------------
print("\n" + "="*80)
print("👥 USER ENGAGEMENT AGGREGATIONS")
print("="*80)

# 1. Monthly Engagement: Total watch time per country and plan
print("\n1️⃣ Computing monthly engagement (watch time per country and plan)...")
monthly_engagement = df_for_agg.groupBy(
    "watch_year",
    "watch_month",
    "country",
    "subscription_plan"
).agg(
    spark_sum("watch_duration_minutes").alias("total_watch_time_minutes"),
    countDistinct("user_id").alias("monthly_active_users"),
    count("*").alias("total_sessions"),
    avg("watch_duration_minutes").alias("avg_session_duration"),
    countDistinct("movie_id").alias("unique_content_viewed")
).orderBy("watch_year", "watch_month", "country", "subscription_plan")

print("✅ Monthly engagement aggregation complete")
print(f"   Rows in monthly_engagement: {monthly_engagement.count()}")
print("\n📊 Sample monthly engagement data:")
monthly_engagement.show(10, truncate=False)

# 2. Monthly Active Users (MAU) - overall
print("\n2️⃣ Computing Monthly Active Users (MAU)...")
mau = df_for_agg.groupBy(
    "watch_year",
    "watch_month"
).agg(
    countDistinct("user_id").alias("monthly_active_users"),
    countDistinct("country").alias("countries"),
    spark_sum("watch_duration_minutes").alias("total_watch_time_minutes")
).orderBy("watch_year", "watch_month")

print("✅ MAU aggregation complete")
print(f"   Rows in MAU: {mau.count()}")
print("\n📊 Monthly Active Users:")
mau.show(20, truncate=False)

# 3. Cohort Retention Analysis
print("\n3️⃣ Computing cohort retention...")
# Get user's first watch date (cohort)
user_cohorts = df_for_agg.groupBy("user_id").agg(
    spark_min("watch_date_parsed").alias("first_watch_date")
).withColumn("cohort_year", year(col("first_watch_date"))) \
 .withColumn("cohort_month", month(col("first_watch_date")))

# Join back to get all user activity
user_activity = df_for_agg.join(
    user_cohorts.select("user_id", "cohort_year", "cohort_month"),
    on="user_id",
    how="inner"
)

# Calculate retention: users active in each month relative to their cohort
cohort_retention = user_activity.groupBy(
    "cohort_year",
    "cohort_month",
    "watch_year",
    "watch_month"
).agg(
    countDistinct("user_id").alias("active_users")
).withColumn(
    "months_since_cohort",
    (col("watch_year") - col("cohort_year")) * 12 + (col("watch_month") - col("cohort_month"))
).orderBy("cohort_year", "cohort_month", "watch_year", "watch_month")

print("✅ Cohort retention aggregation complete")
print(f"   Rows in cohort_retention: {cohort_retention.count()}")
print("\n📊 Sample cohort retention data:")
cohort_retention.show(20, truncate=False)

print("\n✅ All user engagement aggregations complete!")


# In[14]:


# -------------------------------------------------------------------
# 12. Write Aggregated Data to BigQuery
# -------------------------------------------------------------------
print("\n" + "="*80)
print("📤 WRITING AGGREGATED DATA TO BIGQUERY")
print("="*80)

# Write monthly_engagement table (as required by assignment)
print("\n1️⃣ Writing monthly_engagement table...")
monthly_engagement.write.format('bigquery') \
    .option('table', f"{project_id}.{bq_dataset_processed}.monthly_engagement") \
    .mode("overwrite") \
    .save()
print(f"   ✅ monthly_engagement written to {bq_dataset_processed}.monthly_engagement")
print(f"   Rows: {monthly_engagement.count()}")

# Write cohort_retention table (as required by assignment)
print("\n2️⃣ Writing cohort_retention table...")
cohort_retention.write.format('bigquery') \
    .option('table', f"{project_id}.{bq_dataset_processed}.cohort_retention") \
    .mode("overwrite") \
    .save()
print(f"   ✅ cohort_retention written to {bq_dataset_processed}.cohort_retention")
print(f"   Rows: {cohort_retention.count()}")

# Optional: Write additional aggregated tables for dashboard
print("\n3️⃣ Writing additional aggregated tables...")

# Content performance
content_performance.write.format('bigquery') \
    .option('table', f"{project_id}.{bq_dataset_processed}.content_performance") \
    .mode("overwrite") \
    .save()
print(f"   ✅ content_performance written to {bq_dataset_processed}.content_performance")

# Genre performance
genre_performance.write.format('bigquery') \
    .option('table', f"{project_id}.{bq_dataset_processed}.genre_performance") \
    .mode("overwrite") \
    .save()
print(f"   ✅ genre_performance written to {bq_dataset_processed}.genre_performance")

# MAU
mau.write.format('bigquery') \
    .option('table', f"{project_id}.{bq_dataset_processed}.monthly_active_users") \
    .mode("overwrite") \
    .save()
print(f"   ✅ monthly_active_users written to {bq_dataset_processed}.monthly_active_users")

print("\n" + "="*80)
print("🎉 BATCH PIPELINE COMPLETE!")
print("="*80)
print(f"\n✅ All data written to BigQuery dataset: {bq_dataset_processed}")
print("\n📊 Summary of outputs:")
print(f"   - Cleaned tables:")
print(f"     • Users, Movies, WatchHistory, RecommendationLogs, Reviews, SearchLogs")
print(f"   - Aggregated tables:")
print(f"     • monthly_engagement (required)")
print(f"     • cohort_retention (required)")
print(f"     • content_performance (optional)")
print(f"     • genre_performance (optional)")
print(f"     • monthly_active_users (optional)")
print("\n🚀 Ready for Looker Studio dashboard creation!")


# In[15]:


# Stop the Spark context
spark.stop()

