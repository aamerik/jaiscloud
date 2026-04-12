.PHONY: build test clean

PLUGIN_NAME := aws-emr-spark.so
PLUGIN_DIR  := ../../

## build: Compile the plugin as a .so file.
build:
	go build -buildmode=plugin -o $(PLUGIN_DIR)$(PLUGIN_NAME) .

## test: Run unit tests with race detector.
test:
	go test -race ./internal/...

## clean: Remove the built plugin .so file.
clean:
	rm -f $(PLUGIN_DIR)$(PLUGIN_NAME)
