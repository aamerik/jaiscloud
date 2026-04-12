FROM apache/spark:3.5.0

USER root

# Copy pre-downloaded JARs into Spark's jars directory.
# Run download-jars.sh first to populate the jars/ directory.
COPY jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar  /opt/spark/jars/
COPY jars/iceberg-aws-bundle-1.5.2.jar              /opt/spark/jars/
COPY jars/hadoop-aws-3.3.4.jar                      /opt/spark/jars/
COPY jars/aws-java-sdk-bundle-1.12.262.jar           /opt/spark/jars/

USER spark
