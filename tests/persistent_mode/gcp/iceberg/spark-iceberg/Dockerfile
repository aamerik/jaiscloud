FROM apache/spark:3.5.0

USER root

# Copy pre-downloaded JARs into Spark's jars directory.
# Run download-jars.sh first to populate the jars/ directory.
#
# Data path decision (plan_docs/gcp-iceberg-dataproc-e2e.md §7 D1):
# HadoopFileIO + gcs-connector. iceberg-gcp-bundle is intentionally NOT shipped.
COPY jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar /opt/spark/jars/
COPY jars/gcs-connector-hadoop3-2.2.11-shaded.jar   /opt/spark/jars/

USER spark
