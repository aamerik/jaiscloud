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
K8S_NAMESPACE           ?= jaiscloud
JAISCLOUD_K8S_APISERVER ?= $(shell kubectl config view --context docker-desktop --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)
JAISCLOUD_K8S_CA_FILE           ?=
JAISCLOUD_K8S_CLIENT_CERT_FILE  ?=
JAISCLOUD_K8S_CLIENT_KEY_FILE   ?=

# ─── Server knobs ─────────────────────────────────────────────────────────────
JAISCLOUD_DSN  ?= postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud
JAISCLOUD_PORT ?= 4566
JAISCLOUD_HOST ?= http://localhost:$(JAISCLOUD_PORT)

# Narrow any test target to a single test: make test-e2e-emrcontainers-k8s TEST_RUN=TestSparkJob_K8s_CancelJobRun
TEST_RUN ?= .

IMAGE := jaiscloud

.PHONY: help build test docker clean \
        server-lite server-full server-docker server-k8s server-full-all \
        stop-server up-docker down-docker up-k8s down-k8s \
        test-integration \
        test-e2e-emr-docker test-e2e-emrcontainers-k8s test-e2e-eventbridge \
        test-e2e-dpc-docker test-e2e-dpc-k8s \
        test-e2e-lambda-docker test-e2e-lambda-k8s \
        test-e2e-cloudformation test-e2e-kms test-e2e-persistence \
        test-e2e-iceberg \
        test-e2e-docker-all test-e2e-k8s-all test-e2e test-all \
        _build-for-e2e _restart-server-lite _wait-docker _start-k8s _stop-k8s \
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
	@printf "  %-30s  %s\n"  "JAISCLOUD_DSN" "postgres://jaiscloud:jaiscloud@localhost:5433/jaiscloud"
	@printf "  %-30s  %s\n"  "K8S_NAMESPACE" "jaiscloud  — K8s namespace for Spark and Lambda jobs"
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

server-docker: _check-docker-prereq docker ## Full mode + Spark and Lambda via Docker (docker-compose, Ctrl-C to stop)
	JAISCLOUD_EXECUTOR_MODE=$(or $(JAISCLOUD_EXECUTOR_MODE),docker) \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  docker-compose up

server-k8s: _check-k8s-prereq up-k8s ## Full mode + Spark and Lambda via K8s  (requires docker-desktop K8s)
	kubectl port-forward -n jaiscloud svc/jaiscloud $(JAISCLOUD_PORT):4566

server-full-all: server-k8s ## Alias for server-k8s (full mode, all executors via K8s)

stop-server: ## Stop background jaiscloud process and clean up Lambda/Spark resources
	@pkill -f "jaiscloud start" 2>/dev/null && echo "jaiscloud stopped" || echo "jaiscloud was not running"
	@kubectl delete pods -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete svc -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete jobs -l app=jaiscloud-spark -n jaiscloud --ignore-not-found 2>/dev/null || true

##@ Containerized server lifecycle

up-docker: _check-docker-prereq docker ## Start JaisCloud + Postgres via docker-compose (detached)
	JAISCLOUD_EXECUTOR_MODE=$(or $(JAISCLOUD_EXECUTOR_MODE),docker) \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  docker-compose up -d
	$(MAKE) _wait-docker

down-docker: ## Stop and remove docker-compose services
	docker-compose down --remove-orphans

up-k8s: _check-k8s-prereq docker ## Deploy JaisCloud + Postgres to K8s  (docker-desktop)
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/rbac.yaml
	kubectl apply -f deploy/k8s/postgres.yaml
	kubectl apply -f deploy/k8s/jaiscloud.yaml
	@echo "Waiting for jaiscloud deployment..."
	@kubectl rollout status deployment/jaiscloud -n jaiscloud --timeout=120s
	$(MAKE) _wait-docker

down-k8s: ## Remove JaisCloud K8s deployment and clean up Lambda/Spark resources
	@kubectl delete -f deploy/k8s/jaiscloud.yaml --ignore-not-found 2>/dev/null || true
	@kubectl delete pods -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete svc -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete jobs -l app=jaiscloud-spark -n jaiscloud --ignore-not-found 2>/dev/null || true

##@ Integration tests  (lite mode, no postgres required)

test-integration: _restart-server-lite ## Run tests/integration/ — use TEST_RUN=TestSQS to target one service
	# Available TEST_RUN values: TestSQS TestSNS TestDynamoDB TestS3 TestLambda
	#   TestIAM TestKMS TestSecretsManager TestSSM TestCloudFormation
	#   TestAPIGateway TestEventBridge TestEMR TestEMRContainers
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -race -timeout 5m -run "$(TEST_RUN)" ./tests/integration/
	$(MAKE) stop-server

##@ Full-mode e2e tests  (start server, run suite, stop server)

test-e2e-emr-docker: _check-docker-prereq ## EMR Docker Spark tests — tests/full_mode/aws/emr/ (tag: spark_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE)
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/aws/emr/
	$(MAKE) down-docker

test-e2e-emrcontainers-k8s: _check-k8s-prereq ## EMR Containers K8s tests — tests/full_mode/aws/emrcontainers/ (tag: spark_e2e)
	# Individual tests via TEST_RUN:
	#   TestSparkJob_K8s_StartJobRun_And_Complete
	#   TestSparkJob_K8s_CancelJobRun
	#   TestSparkJob_K8s_MultipleJobRuns_Concurrent
	#   TestSparkJob_K8s_FailedJobRun_ReportsFailure
	$(MAKE) _start-k8s
	SPARK_E2E_SPARK_IMAGE=$(SPARK_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/full_mode/aws/emrcontainers/
	$(MAKE) _stop-k8s

test-e2e-eventbridge: ## EventBridge notification tests — tests/full_mode/aws/eventbridge/ (no Docker/K8s)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/aws/eventbridge/
	$(MAKE) down-docker

test-e2e-dpc-docker: _check-docker-prereq ## DPC Spark tests via Docker — tests/full_mode/aws/dpc/ (tag: spark_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE)
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/aws/dpc/
	$(MAKE) down-docker

test-e2e-dpc-k8s: _check-k8s-prereq ## DPC Spark tests via K8s — tests/full_mode/aws/dpc/ (tag: spark_e2e)
	$(MAKE) _start-k8s
	SPARK_E2E_SPARK_IMAGE=$(SPARK_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/full_mode/aws/dpc/
	$(MAKE) _stop-k8s

test-e2e-lambda-docker: _check-docker-prereq ## Lambda Docker tests — tests/full_mode/aws/lambda/ (tag: lambda_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE)
	LAMBDA_E2E_DOCKER_IMAGE=$(LAMBDA_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/aws/lambda/
	$(MAKE) down-docker

test-e2e-lambda-k8s: _check-k8s-prereq ## Lambda K8s tests — tests/full_mode/aws/lambda/ (tag: lambda_e2e)
	$(MAKE) _start-k8s
	LAMBDA_E2E_K8S_IMAGE=$(LAMBDA_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/full_mode/aws/lambda/
	$(MAKE) _stop-k8s

test-e2e-cloudformation: ## CloudFormation e2e tests — tests/full_mode/aws/cloudformation/ (tag: cfn_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags cfn_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/aws/cloudformation/
	$(MAKE) down-docker

test-e2e-kms: ## KMS/SecretsManager/SSM e2e tests — tests/full_mode/aws/kms/ (tag: kms_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags kms_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/full_mode/aws/kms/
	$(MAKE) down-docker

test-e2e-persistence: test-e2e-cloudformation test-e2e-kms ## CloudFormation + KMS persistence tests

test-e2e-iceberg: _check-iceberg-prereq ## Iceberg Glue Catalog tests — tests/full_mode/aws/iceberg/ (tag: iceberg_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	SPARK_E2E_ICEBERG_IMAGE=$(SPARK_E2E_ICEBERG_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags iceberg_e2e -timeout 30m ./tests/full_mode/aws/iceberg/
	$(MAKE) down-docker

##@ Aggregate test targets

test-e2e-docker-all: test-e2e-emr-docker test-e2e-dpc-docker test-e2e-lambda-docker test-e2e-eventbridge ## All Docker-based e2e suites

test-e2e-k8s-all: test-e2e-emrcontainers-k8s test-e2e-dpc-k8s test-e2e-lambda-k8s ## All K8s-based e2e suites

test-e2e: test-e2e-docker-all test-e2e-k8s-all test-e2e-persistence test-e2e-iceberg ## All e2e suites (Docker + K8s + Persistence + Iceberg)

test-all: test test-integration test-e2e ## Unit tests + integration tests + all e2e suites

# ─── Internal helpers (not shown in help) ────────────────────────────────────

_build-for-e2e:
	go build -o jaiscloud ./cmd/jaiscloud/

_wait-docker:
	@echo "Waiting for jaiscloud on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 60 ]; then \
	    echo "ERROR: jaiscloud did not become healthy within 60s"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud ready"

_start-k8s: _check-k8s-prereq up-k8s

_stop-k8s: down-k8s

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
