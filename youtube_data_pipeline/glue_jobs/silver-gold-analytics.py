import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Glue Job: Silver → Gold (Analytics Aggregations)
─────────────────────────────────────────────────
Reads cleansed statistics and reference data from Silver,
joins them, and produces business-level aggregations in the Gold layer.

Gold layer tables are optimized for analytics queries in Athena/QuickSight.

Gold tables produced:
  1. trending_analytics   — Daily trending summaries per region
  2. channel_analytics    — Channel performance metrics
  3. category_analytics   — Category-level trends over time

Job Parameters:
    --JOB_NAME              — Glue job name
    --silver_database       — Silver Glue catalog database
    --gold_bucket           — Gold S3 bucket
    --gold_database         — Gold Glue catalog database
    --silver_stats_table    — (optional) Silver stats table  [default: clean_statistics]
    --silver_ref_table      — (optional) Silver reference table [default: reference_data]
"""

# ── Job Setup ────────────────────────────────────────────────────────────────
# Required args first.
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "gold_bucket",
    "gold_database",
])

# Optional args with defaults — resolve separately so the job doesn't fail
# if they aren't supplied. getResolvedOptions raises if a listed key is absent,
# so we only add optional keys that were actually passed.
_optional_defaults = {
    "silver_stats_table": "clean_statistics",
    "silver_ref_table": "reference_data",
}
for _key, _default in _optional_defaults.items():
    _flag = f"--{_key}"
    if _flag in sys.argv:
        args.update(getResolvedOptions(sys.argv, [_key]))
    else:
        args[_key] = _default

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB = args["gold_database"]
STATS_TABLE = args["silver_stats_table"]
REF_TABLE = args["silver_ref_table"]

logger.info(f"Silver DB: {SILVER_DB}  (stats={STATS_TABLE}, ref={REF_TABLE})")
logger.info(f"Gold: bucket={GOLD_BUCKET}, db={GOLD_DB}")


# ── Read Silver Statistics ──────────────────────────────────────────────────
logger.info("Reading Silver statistics table...")

stats_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name=STATS_TABLE,
    transformation_ctx="stats",
)
stats_df = stats_dyf.toDF()
stats_count = stats_df.count()
logger.info(f"Statistics records: {stats_count}")

if stats_count == 0:
    logger.warn("No statistics records found in Silver. Committing empty job.")
    job.commit()
    sys.exit(0)


# ── Read Reference Data (optional) + Build category lookup ───────────────────
logger.info(f"Attempting to read Silver reference data ({REF_TABLE}) for category names...")

category_lookup = None
try:
    ref_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name=REF_TABLE,
        transformation_ctx="ref",
    )
    ref_df = ref_dyf.toDF()
    ref_cols = ref_df.columns
    logger.info(f"Reference columns found: {ref_cols}")

    # The category title lives under different names depending on how the
    # data was normalised/crawled. Detect it rather than assuming one shape.
    #   - awswrangler json_normalize → "snippet.title" (dotted)
    #   - some crawlers              → "snippet_title" (underscore)
    #   - already-flattened          → "title" / "category_name"
    title_col = None
    for candidate in ["snippet.title", "snippet_title", "category_name", "title"]:
        if candidate in ref_cols:
            title_col = candidate
            break

    if "id" in ref_cols and title_col is not None:
        # Backtick the column in case it contains a dot (Spark needs it escaped).
        title_expr = F.col(f"`{title_col}`") if "." in title_col else F.col(title_col)
        category_lookup = ref_df.select(
            F.col("id").cast("long").alias("category_id"),
            title_expr.alias("category_name"),
        ).dropDuplicates(["category_id"])
        logger.info(f"Category lookup built from '{title_col}'.")
    else:
        logger.warn(
            "Could not find expected category id/title columns in reference data. "
            f"Columns found: {ref_cols}. Proceeding with category_name = 'Unknown'."
        )

except Exception as e:
    logger.warn(f"Could not load reference data: {e}. Proceeding without category names.")


# ── Join category names onto statistics ──────────────────────────────────────
if category_lookup is not None:
    logger.info(f"Category lookup entries: {category_lookup.count()}")
    # Ensure join key types match before joining.
    if "category_id" in stats_df.columns:
        stats_df = stats_df.withColumn("category_id", F.col("category_id").cast("long"))
        stats_df = stats_df.join(
            F.broadcast(category_lookup),
            on="category_id",
            how="left",
        )
    else:
        logger.warn("stats has no category_id column — cannot join category names.")

# Always guarantee a usable category_name column (fill unmatched / missing with 'Unknown').
if "category_name" not in stats_df.columns:
    stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))
else:
    stats_df = stats_df.fillna({"category_name": "Unknown"})

# Cache — stats_df is reused for all three aggregations below.
stats_df = stats_df.cache()


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 1: Trending Analytics (daily summaries per region)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: trending_analytics...")

trending = stats_df.groupBy("region", "trending_date_parsed").agg(
    F.count("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("dislikes").alias("total_dislikes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.avg("like_ratio").alias("avg_like_ratio"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.max("views").alias("max_views"),
    F.countDistinct("channel_title").alias("unique_channels"),
    F.countDistinct("category_id").alias("unique_categories"),
)

trending = trending.withColumn("_aggregated_at", F.current_timestamp())

trending_path = f"s3://{GOLD_BUCKET}/youtube/trending_analytics/"
trending_dyf = DynamicFrame.fromDF(trending, glueContext, "trending")

sink1 = glueContext.getSink(
    connection_type="s3",
    path=trending_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink1.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="trending_analytics")
sink1.setFormat("glueparquet", compression="snappy")
sink1.writeFrame(trending_dyf)
logger.info(f"  Written {trending.count()} rows → {trending_path}")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 2: Channel Analytics
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: channel_analytics...")

channel = stats_df.groupBy("channel_title", "region").agg(
    F.countDistinct("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.max("views").alias("peak_views"),
    F.count("trending_date_parsed").alias("times_trending"),
    F.min("trending_date_parsed").alias("first_trending"),
    F.max("trending_date_parsed").alias("last_trending"),
    F.collect_set("category_name").alias("categories"),
)

# Rank channels by total views within each region.
window_rank = Window.partitionBy("region").orderBy(F.col("total_views").desc())
channel = channel.withColumn("rank_in_region", F.row_number().over(window_rank))
channel = channel.withColumn("_aggregated_at", F.current_timestamp())

channel_path = f"s3://{GOLD_BUCKET}/youtube/channel_analytics/"
channel_dyf = DynamicFrame.fromDF(channel, glueContext, "channel")

sink2 = glueContext.getSink(
    connection_type="s3",
    path=channel_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink2.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="channel_analytics")
sink2.setFormat("glueparquet", compression="snappy")
sink2.writeFrame(channel_dyf)
logger.info(f"  Written {channel.count()} rows → {channel_path}")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 3: Category Analytics (trend over time)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: category_analytics...")

category = stats_df.groupBy(
    "category_name", "category_id", "region", "trending_date_parsed"
).agg(
    F.count("video_id").alias("video_count"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.countDistinct("channel_title").alias("unique_channels"),
)

# Category share of views per region per day.
window_total = Window.partitionBy("region", "trending_date_parsed")
category = category.withColumn(
    "view_share_pct",
    F.round(F.col("total_views") / F.sum("total_views").over(window_total) * 100, 2)
)
category = category.withColumn("_aggregated_at", F.current_timestamp())

category_path = f"s3://{GOLD_BUCKET}/youtube/category_analytics/"
category_dyf = DynamicFrame.fromDF(category, glueContext, "category")

sink3 = glueContext.getSink(
    connection_type="s3",
    path=category_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink3.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="category_analytics")
sink3.setFormat("glueparquet", compression="snappy")
sink3.writeFrame(category_dyf)
logger.info(f"  Written {category.count()} rows → {category_path}")

logger.info("Gold layer build complete.")
job.commit()