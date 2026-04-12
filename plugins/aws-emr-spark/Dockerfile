# Builds the aws-emr-spark plugin .so inside a multi-stage Docker image.
# The resulting .so can be mounted or copied into the jaiscloud container.
#
# Usage:
#   docker build -t jaiscloud-plugin-aws-emr-spark:latest .
#   docker cp $(docker create jaiscloud-plugin-aws-emr-spark:latest):/plugin/aws-emr-spark.so .
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Copy the plugin SDK (referenced via replace directive)
COPY ../../sdk /sdk

# Copy the plugin source
COPY . /build/plugins/aws-emr-spark
WORKDIR /build/plugins/aws-emr-spark

RUN go build -buildmode=plugin -o /plugin/aws-emr-spark.so .

# Final stage: just the .so (alpine for minimal size)
FROM alpine:3.20

COPY --from=builder /plugin/aws-emr-spark.so /plugin/aws-emr-spark.so

# The .so is the artifact; no entrypoint needed.
CMD ["sh", "-c", "echo 'aws-emr-spark plugin built. Copy /plugin/aws-emr-spark.so to your jaiscloud plugin directory.'"]
