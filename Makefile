# ─── Defaults ─────────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

# ─── Version ──────────────────────────────────────────────────────────────────
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || \
             grep -oP 'const version = "\K[^"]+' cmd/jaiscloud/main.go 2>/dev/null || \
             echo "dev")

REGISTRY ?=

# ─── Container images ─────────────────────────────────────────────────────────
# Matches const DefaultImage in internal/executor/spark/config.go
SPARK_IMAGE             ?= apache/spark:3.5.0
# Matches python3.12 entry in internal/executor/lambda/config.go runtimeImages
LAMBDA_IMAGE            ?= public.ecr.aws/lambda/python:3.12
# Custom Iceberg-enabled Spark image (must be built locally before use)
SPARK_E2E_ICEBERG_IMAGE ?= spark-iceberg-test

# ─── K8s configuration ────────────────────────────────────────────────────────
K8S_NAMESPACE           ?= default
JAISCLOUD_K8S_APISERVER ?= $(shell kubectl config view --context docker-desktop --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)
JAISCLOUD_K8S_CA_FILE           ?=
JAISCLOUD_K8S_CLIENT_CERT_FILE  ?=
JAISCLOUD_K8S_CLIENT_KEY_FILE   ?=

# ─── Server knobs ─────────────────────────────────────────────────────────────
JAISCLOUD_DSN  ?= postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud
JAISCLOUD_PORT ?= 4566
JAISCLOUD_HOST ?= http://localhost:$(JAISCLOUD_PORT)

# Narrow any test target to a single test: make test-e2e-emrcontainers-k8s TEST_RUN=TestSparkJob_K8s_CancelJobRun
TEST_RUN ?= .

IMAGE := jaiscloud

.PHONY: help build test docker clean \
        server-lite server-full server-docker server-k8s server-full-all \
        stop-server \
        test-integration \
        test-e2e-emr-docker test-e2e-emrcontainers-k8s test-e2e-eventbridge \
        test-e2e-dpc-docker test-e2e-dpc-k8s \
        test-e2e-lambda-docker test-e2e-lambda-k8s \
        test-e2e-p25 test-e2e-iceberg \
        test-e2e-docker-all test-e2e-k8s-all test-e2e test-all \
        _build-for-e2e _restart-server _restart-server-lite _setup-k8s-creds \
        _check-docker-prereq _check-k8s-prereq _check-iceberg-prereq

# ─── Help ─────────────────────────────────────────────────────────────────────
# NOTE: 'make --help' and 'make -h' show GNU Make's own flags (cannot be overridden).
#       Use 'make help' or bare 'make' to see JaisCloud targets.

help: ## Show this help  (tip: bare 'make' also works)
	@echo ""
	@echo "\033[1mJaisCloud — available targets\033[0m"
	@echo "  Override variables inline, e.g.:  make test-e2e-emr-docker SPARK_IMAGE=my/spark:4.0"
	@echo ""
	@printf "  \033[1m%-30s  %s\033[0m\n" "Target" "Description"
	@printf "  %-30s  %s\n"  "------------------------------" "---------------------------------------------------"
	@awk 'BEGIN { FS = "##" } \
	  /^##@/ { printf "\n  \033[4m%s\033[0m\n", substr($$0, 5) } \
	  /^[a-zA-Z_-][a-zA-Z0-9_-]*:.*##/ { \
	    t = $$1; sub(/:.*/, "", t); gsub(/[[:space:]]/, "", t); \
	    printf "  \033[36m%-30s\033[0m  %s\n", t, $$2 \
	  }' $(MAKEFILE_LIST)
	@echo ""
	@printf "  \033[1m%-30s  %s\033[0m\n" "Variable" "Default / purpose"
	@printf "  %-30s  %s\n"  "------------------------------" "---------------------------------------------------"
	@printf "  %-30s  %s\n"  "SPARK_IMAGE"   "apache/spark:3.5.0  — Spark container image"
	@printf "  %-30s  %s\n"  "LAMBDA_IMAGE"  "public.ecr.aws/lambda/python:3.12  — Lambda image"
	@printf "  %-30s  %s\n"  "JAISCLOUD_DSN" "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
	@printf "  %-30s  %s\n"  "K8S_NAMESPACE" "default  — K8s namespace for Spark and Lambda jobs"
	@printf "  %-30s  %s\n"  "TEST_RUN"      ".  — go test -run filter (any e2e target)"
	@echo ""

##@ Build

build: ## Compile the jaiscloud binary (CGO_ENABLED=0, static)
	go build -trimpath -ldflags="-s -w" -o jaiscloud ./cmd/jaiscloud/

docker: ## Build jaiscloud Docker image tagged jaiscloud:VERSION
	docker build \
		--build-arg VERSION=$(VERSION) \
		--tag $(IMAGE):$(VERSION) \
		--tag $(IMAGE):latest \
		--file Dockerfile \
		.
ifdef REGISTRY
	docker tag $(IMAGE):$(VERSION) $(REGISTRY)/$(IMAGE):$(VERSION)
	docker tag $(IMAGE):latest     $(REGISTRY)/$(IMAGE):latest
endif

clean: ## Remove the compiled binary
	rm -f jaiscloud

##@ Unit tests

test: ## Run all unit tests with the race detector  (no server needed)
	go test -race ./internal/...

##@ Server — foreground (Ctrl-C to stop)

server-lite: build ## In-memory stores, mock executors — no postgres required
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud start

server-full: build ## Postgres stores, mock executors — requires JAISCLOUD_DSN
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud start --mode full --dsn "$(JAISCLOUD_DSN)"

server-docker: build _check-docker-prereq ## Full mode + Spark and Lambda via Docker  (SPARK_IMAGE, LAMBDA_IMAGE)
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  JAISCLOUD_EXECUTOR_MODE=docker \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  ./jaiscloud start --mode full --dsn "$(JAISCLOUD_DSN)"

server-k8s: build _check-k8s-prereq _setup-k8s-creds ## Full mode + Spark and Lambda via K8s  (requires docker-desktop K8s)
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  JAISCLOUD_EXECUTOR_MODE=k8s \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER) \
	  JAISCLOUD_K8S_NAMESPACE=$(K8S_NAMESPACE) \
	  JAISCLOUD_K8S_CA_FILE=/tmp/jaiscloud-k8s-ca.crt \
	  JAISCLOUD_K8S_CLIENT_CERT_FILE=/tmp/jaiscloud-k8s-client.crt \
	  JAISCLOUD_K8S_CLIENT_KEY_FILE=/tmp/jaiscloud-k8s-client.key \
	  ./jaiscloud start --mode full --dsn "$(JAISCLOUD_DSN)"

server-full-all: server-k8s ## Alias for server-k8s (full mode, all executors via K8s)

stop-server: ## Stop the background jaiscloud instance started by e2e targets
	@pkill -f "jaiscloud start" 2>/dev/null && echo "jaiscloud stopped" || echo "jaiscloud was not running"

##@ Integration tests  (lite mode, no postgres required)

test-integration: _restart-server-lite ## Run tests/integration/ — use TEST_RUN=TestSQS to target one service
	# Available TEST_RUN values: TestSQS TestSNS TestDynamoDB TestS3 TestLambda
	#   TestIAM TestKMS TestSecretsManager TestSSM TestCloudFormation
	#   TestAPIGateway TestEventBridge TestEMR TestEMRContainers
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -race -timeout 5m -run "$(TEST_RUN)" ./tests/integration/
	$(MAKE) stop-server

##@ Full-mode e2e tests  (start server, run suite, stop server)

test-e2e-emr-docker: _check-docker-prereq ## EMR Docker Spark tests — tests/full_mode/emr/ (4 tests, tag: spark_e2e)
	$(MAKE) _restart-server JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE)
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/emr/
	$(MAKE) stop-server

test-e2e-emrcontainers-k8s: _check-k8s-prereq _setup-k8s-creds ## EMR Containers K8s tests — tests/full_mode/emrcontainers/ (4 tests, tag: spark_e2e)
	# Individual tests via TEST_RUN:
	#   TestSparkJob_K8s_StartJobRun_And_Complete
	#   TestSparkJob_K8s_CancelJobRun
	#   TestSparkJob_K8s_MultipleJobRuns_Concurrent
	#   TestSparkJob_K8s_FailedJobRun_ReportsFailure
	$(MAKE) _restart-server \
	  JAISCLOUD_EXECUTOR_MODE=k8s \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER) \
	  JAISCLOUD_K8S_NAMESPACE=$(K8S_NAMESPACE) \
	  JAISCLOUD_K8S_CA_FILE=/tmp/jaiscloud-k8s-ca.crt \
	  JAISCLOUD_K8S_CLIENT_CERT_FILE=/tmp/jaiscloud-k8s-client.crt \
	  JAISCLOUD_K8S_CLIENT_KEY_FILE=/tmp/jaiscloud-k8s-client.key
	SPARK_E2E_SPARK_IMAGE=$(SPARK_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/full_mode/emrcontainers/
	$(MAKE) stop-server

test-e2e-eventbridge: ## EventBridge notification tests — tests/full_mode/eventbridge/ (5 tests, no Docker/K8s)
	$(MAKE) _restart-server JAISCLOUD_EXECUTOR_MODE=mock
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/eventbridge/
	$(MAKE) stop-server

test-e2e-dpc-docker: _check-docker-prereq ## DPC Spark tests via Docker — tests/full_mode/dpc/ (2 tests, tag: spark_e2e)
	$(MAKE) _restart-server JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE)
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/dpc/
	$(MAKE) stop-server

test-e2e-dpc-k8s: _check-k8s-prereq _setup-k8s-creds ## DPC Spark tests via K8s — tests/full_mode/dpc/ (2 tests, tag: spark_e2e)
	$(MAKE) _restart-server \
	  JAISCLOUD_EXECUTOR_MODE=k8s \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER) \
	  JAISCLOUD_K8S_NAMESPACE=$(K8S_NAMESPACE) \
	  JAISCLOUD_K8S_CA_FILE=/tmp/jaiscloud-k8s-ca.crt \
	  JAISCLOUD_K8S_CLIENT_CERT_FILE=/tmp/jaiscloud-k8s-client.crt \
	  JAISCLOUD_K8S_CLIENT_KEY_FILE=/tmp/jaiscloud-k8s-client.key
	SPARK_E2E_SPARK_IMAGE=$(SPARK_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/full_mode/dpc/
	$(MAKE) stop-server

test-e2e-lambda-docker: _check-docker-prereq ## Lambda Docker tests — tests/full_mode/p25/ (tag: lambda_e2e, needs RIC image)
	$(MAKE) _restart-server JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE)
	LAMBDA_E2E_DOCKER_IMAGE=$(LAMBDA_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 10m -run "TestLambda_Docker|$(TEST_RUN)" ./tests/full_mode/p25/
	$(MAKE) stop-server

test-e2e-lambda-k8s: _check-k8s-prereq _setup-k8s-creds ## Lambda K8s tests — tests/full_mode/p25/ (tag: lambda_e2e)
	$(MAKE) _restart-server \
	  JAISCLOUD_EXECUTOR_MODE=k8s \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER) \
	  JAISCLOUD_K8S_NAMESPACE=$(K8S_NAMESPACE) \
	  JAISCLOUD_K8S_CA_FILE=/tmp/jaiscloud-k8s-ca.crt \
	  JAISCLOUD_K8S_CLIENT_CERT_FILE=/tmp/jaiscloud-k8s-client.crt \
	  JAISCLOUD_K8S_CLIENT_KEY_FILE=/tmp/jaiscloud-k8s-client.key
	LAMBDA_E2E_K8S_IMAGE=$(LAMBDA_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 15m -run "TestLambda_K8s|$(TEST_RUN)" ./tests/full_mode/p25/
	$(MAKE) stop-server

test-e2e-p25: ## Phase 2.5 tests: CloudFormation, KMS, SecretsManager, SSM — tests/full_mode/p25/ (no Docker/K8s)
	$(MAKE) _restart-server JAISCLOUD_EXECUTOR_MODE=mock
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/p25/
	$(MAKE) stop-server

test-e2e-iceberg: _check-iceberg-prereq ## Iceberg Glue Catalog tests — tests/full_mode/iceberg/ (6 tests, tag: iceberg_e2e)
	$(MAKE) _restart-server JAISCLOUD_EXECUTOR_MODE=mock
	SPARK_E2E_ICEBERG_IMAGE=$(SPARK_E2E_ICEBERG_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags iceberg_e2e -timeout 30m ./tests/full_mode/iceberg/
	$(MAKE) stop-server

##@ Aggregate test targets

test-e2e-docker-all: test-e2e-emr-docker test-e2e-dpc-docker test-e2e-lambda-docker test-e2e-eventbridge ## All Docker-based e2e suites

test-e2e-k8s-all: test-e2e-emrcontainers-k8s test-e2e-dpc-k8s test-e2e-lambda-k8s ## All K8s-based e2e suites

test-e2e: test-e2e-docker-all test-e2e-k8s-all test-e2e-p25 test-e2e-iceberg ## All e2e suites (Docker + K8s + EventBridge + Iceberg)

test-all: test test-integration test-e2e ## Unit tests + integration tests + all e2e suites

# ─── Internal helpers (not shown in help) ────────────────────────────────────

_build-for-e2e:
	go build -o jaiscloud ./cmd/jaiscloud/

_setup-k8s-creds:
	@kubectl config view --context docker-desktop --minify --raw \
	  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' \
	  | base64 -d > /tmp/jaiscloud-k8s-ca.crt
	@kubectl config view --context docker-desktop --minify --raw \
	  -o jsonpath='{.users[0].user.client-certificate-data}' \
	  | base64 -d > /tmp/jaiscloud-k8s-client.crt
	@kubectl config view --context docker-desktop --minify --raw \
	  -o jsonpath='{.users[0].user.client-key-data}' \
	  | base64 -d > /tmp/jaiscloud-k8s-client.key
	@chmod 600 /tmp/jaiscloud-k8s-client.key
	@echo "K8s credentials written to /tmp/jaiscloud-k8s-{ca,client}.{crt,key}"

_restart-server: _build-for-e2e
	@pkill -f "jaiscloud start" 2>/dev/null || true
	@sleep 1
	@JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  $(if $(JAISCLOUD_EXECUTOR_MODE),JAISCLOUD_EXECUTOR_MODE=$(JAISCLOUD_EXECUTOR_MODE)) \
	  $(if $(JAISCLOUD_SPARK_IMAGE),JAISCLOUD_SPARK_IMAGE=$(JAISCLOUD_SPARK_IMAGE)) \
	  $(if $(JAISCLOUD_LAMBDA_IMAGE),JAISCLOUD_LAMBDA_IMAGE=$(JAISCLOUD_LAMBDA_IMAGE)) \
	  $(if $(JAISCLOUD_K8S_APISERVER),JAISCLOUD_K8S_APISERVER=$(JAISCLOUD_K8S_APISERVER)) \
	  $(if $(JAISCLOUD_K8S_NAMESPACE),JAISCLOUD_K8S_NAMESPACE=$(JAISCLOUD_K8S_NAMESPACE)) \
	  $(if $(JAISCLOUD_K8S_CA_FILE),JAISCLOUD_K8S_CA_FILE=$(JAISCLOUD_K8S_CA_FILE)) \
	  $(if $(JAISCLOUD_K8S_CLIENT_CERT_FILE),JAISCLOUD_K8S_CLIENT_CERT_FILE=$(JAISCLOUD_K8S_CLIENT_CERT_FILE)) \
	  $(if $(JAISCLOUD_K8S_CLIENT_KEY_FILE),JAISCLOUD_K8S_CLIENT_KEY_FILE=$(JAISCLOUD_K8S_CLIENT_KEY_FILE)) \
	  ./jaiscloud start --mode full --dsn "$(JAISCLOUD_DSN)" \
	  > /tmp/jaiscloud-e2e.log 2>&1 &
	@echo "Waiting for jaiscloud on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 30 ]; then \
	    echo "ERROR: jaiscloud did not become healthy — check /tmp/jaiscloud-e2e.log"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud ready (log: /tmp/jaiscloud-e2e.log)"

_restart-server-lite: _build-for-e2e
	@pkill -f "jaiscloud start" 2>/dev/null || true
	@sleep 1
	@JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud start \
	  > /tmp/jaiscloud-e2e.log 2>&1 &
	@echo "Waiting for jaiscloud on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 30 ]; then \
	    echo "ERROR: jaiscloud did not become healthy — check /tmp/jaiscloud-e2e.log"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud ready (log: /tmp/jaiscloud-e2e.log)"

_check-docker-prereq:
	@docker info > /dev/null 2>&1 || \
	  (echo "ERROR: Docker daemon is not running"; exit 1)

_check-k8s-prereq:
	@kubectl --context docker-desktop cluster-info > /dev/null 2>&1 || \
	  (echo "ERROR: docker-desktop Kubernetes is not reachable — enable Kubernetes in Docker Desktop"; exit 1)

_check-iceberg-prereq:
	@docker image inspect $(SPARK_E2E_ICEBERG_IMAGE) > /dev/null 2>&1 || \
	  (echo "ERROR: image '$(SPARK_E2E_ICEBERG_IMAGE)' not found — build or pull it first"; exit 1)
