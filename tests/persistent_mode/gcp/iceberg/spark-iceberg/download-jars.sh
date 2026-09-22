#!/bin/bash
# Download Iceberg and GCS JARs from Maven Central into ./jars/
# Run once before building the Docker image:
#
#   bash download-jars.sh
#   docker build -t spark-iceberg-gcp-test .
#
set -euo pipefail

DEST="$(dirname "$0")/jars"
mkdir -p "$DEST"

BASE="https://repo1.maven.org/maven2"

download() {
    local url="$1"
    local dest="$2"
    if [ -f "$dest" ]; then
        echo "  already exists: $(basename "$dest")"
        return
    fi
    echo "  downloading: $(basename "$dest")"
    curl -fL --progress-bar -o "$dest" "$url"
}

echo "Downloading Iceberg JARs to $DEST ..."

download \
    "$BASE/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
    "$DEST/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

# gcs-connector hadoop3 (shaded). Data path is HadoopFileIO + gcs-connector;
# iceberg-gcp-bundle is not used.
#
# TODO(unconfirmed pin): gcs-connector-hadoop3-2.2.11-shaded.jar is the
# starting pin only. It must be confirmed empirically via the spark-sql boot
# spike (spark-sql boots with it on the classpath + a gs:// round-trip works)
# before the full harness runs. The spike cannot run here (needs Docker Spark
# + the Hive Metastore Thrift listener).
download \
    "$BASE/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11-shaded.jar" \
    "$DEST/gcs-connector-hadoop3-2.2.11-shaded.jar"

echo ""
echo "Done. Build the image with:"
echo "  docker build -t spark-iceberg-gcp-test $(dirname "$0")"
