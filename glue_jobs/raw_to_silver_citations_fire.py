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
# Read RAW Fire Safety Citations table
# -------------------------------------------------
df = glueContext.create_dynamic_frame.from_catalog(
    database="cms_nh_raw",
    table_name="raw_nh_firesafetycitations_oct2024_csv"
).toDF()

# -------------------------------------------------
# Normalize CCN
# -------------------------------------------------
df = df.withColumn(
    "ccn",
    F.lpad(
        F.regexp_replace(
            F.col("CMS Certification Number (CCN)").cast("string"),
            "[^0-9]",
            ""
        ),
        6,
        "0"
    )
)

# -------------------------------------------------
# Select & standardize columns
# -------------------------------------------------
df = df.select(
    F.col("ccn"),
    F.col("State").alias("state"),
    F.col("Survey Date").alias("survey_date_raw"),
    F.col("Correction Date").alias("correction_date_raw"),
    F.col("Deficiency Prefix").alias("deficiency_prefix"),
    F.col("Deficiency Tag Number").cast("int").alias("deficiency_tag_number"),
    F.col("Scope Severity Code").alias("scope_severity_code"),
    F.col("Deficiency Category").alias("deficiency_category"),
    F.col("Standard Deficiency").alias("standard_deficiency"),
    F.col("Complaint Deficiency").alias("complaint_deficiency"),
    F.col("Processing Date").alias("processing_date_raw")
)

# -------------------------------------------------
# Parse dates
# -------------------------------------------------
df = (
    df
    .withColumn("survey_date", F.to_date("survey_date_raw"))
    .withColumn("correction_date", F.to_date("correction_date_raw"))
    .withColumn("processing_date", F.to_date("processing_date_raw"))
    .drop("survey_date_raw", "correction_date_raw", "processing_date_raw")
)

# -------------------------------------------------
# Write SILVER Parquet
# -------------------------------------------------
(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("state")
    .save("s3://cms-nursing-home-analytics/cms_nh/silver/citations_fire/")
)

job.commit()
