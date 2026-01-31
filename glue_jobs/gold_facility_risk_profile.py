import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import pyspark.sql.functions as F

# -------------------------------------------------
# Init
# -------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------------------------------
# Read GOLD citations
# -------------------------------------------------
cit_df = spark.read.parquet(
    "s3://cms-nursing-home-analytics/cms_nh/gold/citations_enriched/"
)

# -------------------------------------------------
# Map severity to numeric weight
# -------------------------------------------------
cit_df = cit_df.withColumn(
    "severity_weight",
    F.when(F.col("severity_level") == "Low", 1)
     .when(F.col("severity_level") == "Moderate", 2)
     .when(F.col("severity_level") == "High", 3)
     .when(F.col("severity_level") == "Immediate Jeopardy", 5)
     .otherwise(0)
)

# -------------------------------------------------
# Aggregate to facility level
# -------------------------------------------------
risk_df = cit_df.groupBy("ccn").agg(
    F.count("*").alias("total_citations"),
    F.sum(F.when(F.col("citation_type") == "HEALTH", 1).otherwise(0)).alias("health_citations"),
    F.sum(F.when(F.col("citation_type") == "FIRE", 1).otherwise(0)).alias("fire_citations"),
    F.sum(F.when(F.col("infection_control_deficiency").isNotNull(), 1).otherwise(0)).alias("infection_control_citations"),
    F.sum("severity_weight").alias("severity_weighted_score")
)

risk_df = risk_df.withColumn(
    "infection_control_rate",
    F.when(F.col("total_citations") > 0,
           F.col("infection_control_citations") / F.col("total_citations"))
     .otherwise(0)
)

# -------------------------------------------------
# Read provider master
# -------------------------------------------------
prov_df = spark.read.parquet(
    "s3://cms-nursing-home-analytics/cms_nh/silver/providerinfo/"
).select(
    "ccn",
    "provider_name",
    "state",
    "certified_beds",
    "overall_rating"
)

# -------------------------------------------------
# Join risk metrics with provider info
# -------------------------------------------------
final_df = risk_df.join(prov_df, on="ccn", how="left")

# -------------------------------------------------
# Write GOLD Parquet
# -------------------------------------------------
(
    final_df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("state")
    .save("s3://cms-nursing-home-analytics/cms_nh/gold/facility_risk_profile/")
)

job.commit()
