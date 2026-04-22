#!/bin/bash
# Download Iceberg and AWS JARs from Maven Central into ./jars/
# Run once before building the Docker image:
#
#   bash download-jars.sh
#   docker build -t spark-iceberg-test .
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

download \
    "$BASE/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar" \
    "$DEST/iceberg-aws-bundle-1.5.2.jar"

download \
    "$BASE/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    "$DEST/hadoop-aws-3.3.4.jar"

download \
    "$BASE/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    "$DEST/aws-java-sdk-bundle-1.12.262.jar"

echo ""
echo "Done. Build the image with:"
echo "  docker build -t spark-iceberg-test $(dirname "$0")"
