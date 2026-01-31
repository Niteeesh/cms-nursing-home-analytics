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
# Read SILVER citation tables
# -------------------------------------------------
health_df = (
    spark.read.parquet("s3://cms-nursing-home-analytics/cms_nh/silver/citations_health/")
    .withColumn("citation_type", F.lit("HEALTH"))
)

fire_df = (
    spark.read.parquet("s3://cms-nursing-home-analytics/cms_nh/silver/citations_fire/")
    .withColumn("citation_type", F.lit("FIRE"))
    .withColumn("infection_control_deficiency", F.lit(None).cast("string"))
)

citations_df = health_df.unionByName(fire_df))

# -------------------------------------------------
# Read RAW citation descriptions (lookup)
# -------------------------------------------------
desc_df = glueContext.create_dynamic_frame.from_catalog(
    database="cms_nh_raw",
    table_name="raw_nh_citationdescriptions_oct2024_csv"
).toDF()

desc_df = desc_df.select(
    F.col("Deficiency Prefix").alias("deficiency_prefix"),
    F.col("Deficiency Tag Number").cast("int").alias("deficiency_tag_number"),
    F.col("Deficiency Description").alias("deficiency_description"),
    F.col("Deficiency Category").alias("deficiency_category")
)

# -------------------------------------------------
# Join citations with descriptions
# -------------------------------------------------
df = (
    citations_df.alias("c")
    .join(
        desc_df.alias("d"),
        on=[
            F.col("c.deficiency_prefix") == F.col("d.deficiency_prefix"),
            F.col("c.deficiency_tag_number") == F.col("d.deficiency_tag_number")
        ],
        how="left"
    )
)

# -------------------------------------------------
# Derive severity level
# -------------------------------------------------
df = df.withColumn(
    "severity_level",
    F.when(F.col("scope_severity_code").isin("A", "B", "C"), "Low")
     .when(F.col("scope_severity_code").isin("D", "E", "F"), "Moderate")
     .when(F.col("scope_severity_code").isin("G", "H", "I"), "High")
     .when(F.col("scope_severity_code").isin("J", "K", "L"), "Immediate Jeopardy")
     .otherwise("Unknown")
)

# -------------------------------------------------
# Final column selection
# -------------------------------------------------
df = df.select(
    F.col("c.ccn").alias("ccn"),
    F.col("c.state").alias("state"),
    F.col("c.citation_type").alias("citation_type"),
    F.col("c.survey_date").alias("survey_date"),
    F.col("c.correction_date").alias("correction_date"),
    F.col("c.deficiency_prefix").alias("deficiency_prefix"),
    F.col("c.deficiency_tag_number").alias("deficiency_tag_number"),
    F.concat(
        F.col("c.deficiency_prefix"),
        F.lpad(F.col("c.deficiency_tag_number").cast("string"), 3, "0")
    ).alias("deficiency_code"),
    F.col("c.scope_severity_code").alias("scope_severity_code"),
    F.col("severity_level"),
    F.col("d.deficiency_category").alias("deficiency_category"),
    F.col("d.deficiency_description").alias("deficiency_description"),
    F.col("c.infection_control_deficiency").alias("infection_control_deficiency"),
    F.col("c.standard_deficiency").alias("standard_deficiency"),
    F.col("c.complaint_deficiency").alias("complaint_deficiency"),
    F.col("c.processing_date").alias("processing_date")
)


# -------------------------------------------------
# Write GOLD Parquet
# -------------------------------------------------
(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("state")
    .save("s3://cms-nursing-home-analytics/cms_nh/gold/citations_enriched/")
)

job.commit()
