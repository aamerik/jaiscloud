# ─── Version ──────────────────────────────────────────────────────────────────
# Inferred from git tags at build time; override with: make docker-lite VERSION=1.2.3
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || \
             grep -oP 'const version = "\K[^"]+' cmd/jaiscloud/main.go 2>/dev/null || \
             echo "dev")

# Optional registry prefix, e.g. REGISTRY=ghcr.io/myorg
# When set, images are tagged as both <image>:<version> and <registry>/<image>:<version>.
REGISTRY ?=

# ─── E2E test knobs ───────────────────────────────────────────────────────────
# Spark image for EMR (Docker) tests.  Override: make test-e2e-docker SPARK_E2E_DOCKER_IMAGE=my/spark:3.5
SPARK_E2E_DOCKER_IMAGE  ?= apache/spark:3.5.0
# Iceberg-enabled Spark image for iceberg tests.  Override: make test-e2e-iceberg SPARK_E2E_ICEBERG_IMAGE=my/iceberg:latest
SPARK_E2E_ICEBERG_IMAGE ?= spark-iceberg-test
# Spark image for EMR Containers (K8s) tests — no default; must be set explicitly.
SPARK_E2E_SPARK_IMAGE   ?=
# K8s namespace used by EMR Containers tests against docker-desktop.
SPARK_E2E_K8S_NAMESPACE ?= default

# ─── Server knobs (used when e2e targets start jaiscloud) ─────────────────────
JAISCLOUD_DSN  ?= postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud
JAISCLOUD_PORT ?= 4566
JAISCLOUD_HOST ?= http://localhost:$(JAISCLOUD_PORT)

LITE_IMAGE := jaiscloud-lite
FULL_IMAGE := jaiscloud-full
SDK_IMAGE  := jaiscloud-sdk

.PHONY: help build plugin test test-plugin test-sdk \
        docker-lite docker-full docker-sdk docker-all clean \
        test-e2e-docker test-e2e-k8s test-e2e-eventbridge test-e2e-spark test-e2e-iceberg test-e2e \
        stop-server \
        _check-docker-prereq _check-k8s-prereq _check-iceberg-prereq \
        _build-for-e2e _restart-server

## help: Print this help.
help:
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':' | sed 's/^/  /'

# ─── Binary ───────────────────────────────────────────────────────────────────

## build: Compile the jaiscloud binary (CGO_ENABLED=0, static).
build:
	go build -trimpath -ldflags="-s -w" -o jaiscloud ./cmd/jaiscloud/

## plugin: Build the aws-emr-spark plugin .so (requires CGO + gcc).
plugin:
	cd plugins/aws-emr-spark && go build -buildmode=plugin -o ../../aws-emr-spark.so .

# ─── Tests ────────────────────────────────────────────────────────────────────

## test: Run host-module unit tests with the race detector.
test:
	go test -race ./internal/...

## test-plugin: Run aws-emr-spark plugin unit tests.
test-plugin:
	cd plugins/aws-emr-spark && go test -race ./internal/...

## test-sdk: Run plugin SDK unit tests.
test-sdk:
	cd sdk && go test -race ./...

# ─── Docker ───────────────────────────────────────────────────────────────────

## docker-lite: Build jaiscloud-lite:<VERSION> — scratch-based, in-memory only, no plugin support.
##              Use this image when you only need lite mode (no PostgreSQL, no plugins).
docker-lite:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--tag $(LITE_IMAGE):$(VERSION) \
		--tag $(LITE_IMAGE):latest \
		--file Dockerfile \
		.
ifdef REGISTRY
	docker tag $(LITE_IMAGE):$(VERSION) $(REGISTRY)/$(LITE_IMAGE):$(VERSION)
	docker tag $(LITE_IMAGE):latest     $(REGISTRY)/$(LITE_IMAGE):latest
endif

## docker-full: Build jaiscloud:<VERSION> — full image with PostgreSQL support and the
##              aws-emr-spark plugin bundled. Starts with --plugin-dir /plugins by default.
docker-full:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--tag $(FULL_IMAGE):$(VERSION) \
		--tag $(FULL_IMAGE):latest \
		--file Dockerfile.full \
		.
ifdef REGISTRY
	docker tag $(FULL_IMAGE):$(VERSION) $(REGISTRY)/$(FULL_IMAGE):$(VERSION)
	docker tag $(FULL_IMAGE):latest     $(REGISTRY)/$(FULL_IMAGE):latest
endif

## docker-sdk: Build jaiscloud-sdk:<VERSION> — Go builder image for developing custom plugins.
##             Mount your plugin source at /workspace and run go build inside.
docker-sdk:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--tag $(SDK_IMAGE):$(VERSION) \
		--tag $(SDK_IMAGE):latest \
		--file Dockerfile.sdk \
		.
ifdef REGISTRY
	docker tag $(SDK_IMAGE):$(VERSION) $(REGISTRY)/$(SDK_IMAGE):$(VERSION)
	docker tag $(SDK_IMAGE):latest     $(REGISTRY)/$(SDK_IMAGE):latest
endif

## docker-all: Build all three images (lite, full, sdk).
docker-all: docker-lite docker-full docker-sdk

# ─── Misc ─────────────────────────────────────────────────────────────────────

## clean: Remove the built binary and plugin .so.
clean:
	rm -f jaiscloud aws-emr-spark.so

# ─── E2E Tests ────────────────────────────────────────────────────────────────

# Internal: build binary + plugin from current source.
# Note: no -trimpath/-ldflags here — trimpath changes package fingerprints and
# breaks plugin loading at runtime (internal/goarch mismatch).
_build-for-e2e:
	go build -o jaiscloud ./cmd/jaiscloud/
	cd plugins/aws-emr-spark && go build -buildmode=plugin -o ../../aws-emr-spark.so .

# Internal: stop any running instance, start a fresh one, wait for health.
# The server keeps running after tests so logs stay accessible; call stop-server to clean up.
_restart-server: _build-for-e2e
	@pkill -f "jaiscloud start" 2>/dev/null || true
	@sleep 1
	@JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud start --mode full --dsn "$(JAISCLOUD_DSN)" --plugin-dir . \
	  > /tmp/jaiscloud-e2e.log 2>&1 &
	@echo "Waiting for jaiscloud to be ready on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 30 ]; then \
	    echo "ERROR: jaiscloud did not become healthy within 30s — check /tmp/jaiscloud-e2e.log"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud ready (log: /tmp/jaiscloud-e2e.log)"

## stop-server: Stop the background jaiscloud instance started by e2e targets.
stop-server:
	@pkill -f "jaiscloud start" 2>/dev/null && echo "jaiscloud stopped" || echo "jaiscloud was not running"

# Internal prereq: Docker daemon running and SPARK_E2E_DOCKER_IMAGE set.
_check-docker-prereq:
	@if [ -z "$(SPARK_E2E_DOCKER_IMAGE)" ]; then \
	  echo "ERROR: SPARK_E2E_DOCKER_IMAGE is not set"; exit 1; fi
	@docker info > /dev/null 2>&1 || \
	  (echo "ERROR: Docker daemon is not running"; exit 1)

# Internal prereq: docker-desktop K8s reachable and SPARK_E2E_SPARK_IMAGE set.
_check-k8s-prereq:
	@if [ -z "$(SPARK_E2E_SPARK_IMAGE)" ]; then \
	  echo "ERROR: SPARK_E2E_SPARK_IMAGE is not set (must be an image loaded in docker-desktop)"; exit 1; fi
	@kubectl --context docker-desktop cluster-info > /dev/null 2>&1 || \
	  (echo "ERROR: docker-desktop Kubernetes cluster is not reachable (is Kubernetes enabled in Docker Desktop?)"; exit 1)

# Internal prereq: iceberg image exists locally.
_check-iceberg-prereq:
	@if [ -z "$(SPARK_E2E_ICEBERG_IMAGE)" ]; then \
	  echo "ERROR: SPARK_E2E_ICEBERG_IMAGE is not set"; exit 1; fi
	@docker image inspect $(SPARK_E2E_ICEBERG_IMAGE) > /dev/null 2>&1 || \
	  (echo "ERROR: image '$(SPARK_E2E_ICEBERG_IMAGE)' not found locally — build or pull it first"; exit 1)

## test-e2e-docker: Run EMR (Docker) Spark e2e tests.
##                  Tests: TestSparkJob_Docker_* (4 tests)
##                  Requires: Docker daemon, SPARK_E2E_DOCKER_IMAGE (default: apache/spark:3.5.0)
test-e2e-docker: _check-docker-prereq _restart-server
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_E2E_DOCKER_IMAGE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m \
	  -run TestSparkJob_Docker \
	  ./tests/full_mode/plugin/

## test-e2e-k8s: Run EMR Containers (K8s) Spark e2e tests against docker-desktop.
##               Tests: TestSparkJob_K8s_* (4 tests)
##               Requires: docker-desktop Kubernetes enabled, SPARK_E2E_SPARK_IMAGE set explicitly.
##               Example: make test-e2e-k8s SPARK_E2E_SPARK_IMAGE=apache/spark:3.5.0
test-e2e-k8s: _check-k8s-prereq _restart-server
	SPARK_E2E_SPARK_IMAGE=$(SPARK_E2E_SPARK_IMAGE) \
	SPARK_E2E_K8S_NAMESPACE=$(SPARK_E2E_K8S_NAMESPACE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m \
	  -run TestSparkJob_K8s \
	  ./tests/full_mode/plugin/

## test-e2e-eventbridge: Run EventBridge notification e2e tests (mock executor — no Docker/K8s needed).
##                       Tests: TestSparkJob_EventBridge_* (5 tests)
test-e2e-eventbridge: _restart-server
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m \
	  -run TestSparkJob_EventBridge \
	  ./tests/full_mode/plugin/

## test-e2e-spark: Run all Spark plugin e2e tests (Docker + EventBridge always; K8s if SPARK_E2E_SPARK_IMAGE set).
##                 Tests: all 13 TestSparkJob_* tests
##                 Requires: Docker daemon, SPARK_E2E_DOCKER_IMAGE (default: apache/spark:3.5.0)
test-e2e-spark: _check-docker-prereq _restart-server
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_E2E_DOCKER_IMAGE) \
	SPARK_E2E_SPARK_IMAGE=$(SPARK_E2E_SPARK_IMAGE) \
	SPARK_E2E_K8S_NAMESPACE=$(SPARK_E2E_K8S_NAMESPACE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 20m \
	  ./tests/full_mode/plugin/

## test-e2e-iceberg: Run all Iceberg e2e tests.
##                   Tests: TestIceberg_GlueCatalog_* (6 tests)
##                   Requires: SPARK_E2E_ICEBERG_IMAGE present locally (default: spark-iceberg-test)
test-e2e-iceberg: _check-iceberg-prereq _restart-server
	SPARK_E2E_ICEBERG_IMAGE=$(SPARK_E2E_ICEBERG_IMAGE) \
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags iceberg_e2e -timeout 30m \
	  ./tests/full_mode/iceberg/

## test-e2e: Run all e2e tests: Spark plugin suite then Iceberg suite.
test-e2e: test-e2e-spark test-e2e-iceberg
