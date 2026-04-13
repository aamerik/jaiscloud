# ─── Version ──────────────────────────────────────────────────────────────────
# Inferred from git tags at build time; override with: make docker-lite VERSION=1.2.3
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || \
             grep -oP 'const version = "\K[^"]+' cmd/jaiscloud/main.go 2>/dev/null || \
             echo "dev")

# Optional registry prefix, e.g. REGISTRY=ghcr.io/myorg
# When set, images are tagged as both <image>:<version> and <registry>/<image>:<version>.
REGISTRY ?=

LITE_IMAGE := jaiscloud-lite
FULL_IMAGE := jaiscloud-full
SDK_IMAGE  := jaiscloud-sdk

.PHONY: help build plugin test test-plugin test-sdk \
        docker-lite docker-full docker-sdk docker-all clean

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
