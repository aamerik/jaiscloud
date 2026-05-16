package admin

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// LambdaCodeFetcher is the interface the admin handler uses to retrieve Lambda
// function code and layer zip bytes.
type LambdaCodeFetcher interface {
	// GetFunctionCodeZip returns the zip bytes for the named function (qualifier
	// may be "$LATEST" or a version number).
	GetFunctionCodeZip(ctx context.Context, account, functionName, qualifier string) ([]byte, error)
	// GetLayerCodeZip returns the zip bytes for the given layer name and version.
	GetLayerCodeZip(ctx context.Context, account, layerName string, version int64) ([]byte, error)
}

// SetLambdaCodeFetcher wires the Lambda code fetcher into the admin handler.
// Call this after constructing the handler but before starting the server.
func (h *Handler) SetLambdaCodeFetcher(f LambdaCodeFetcher) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lambdaCode = f
}

// LambdaCodeHandler handles GET /_jaiscloud/lambda/code/{account}/{function}/{qualifier}
func (h *Handler) LambdaCodeHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	fetcher := h.lambdaCode
	h.mu.Unlock()

	if fetcher == nil {
		http.Error(w, "lambda code fetcher not configured", http.StatusNotImplemented)
		return
	}

	account := chi.URLParam(r, "account")
	function := chi.URLParam(r, "function")
	qualifier := chi.URLParam(r, "qualifier")
	if qualifier == "" {
		qualifier = "$LATEST"
	}

	data, err := fetcher.GetFunctionCodeZip(r.Context(), account, function, qualifier)
	if err != nil {
		http.Error(w, fmt.Sprintf("function code not found: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// LambdaLayerHandler handles GET /_jaiscloud/lambda/layer/{account}/{layer}/{version}
func (h *Handler) LambdaLayerHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	fetcher := h.lambdaCode
	h.mu.Unlock()

	if fetcher == nil {
		http.Error(w, "lambda code fetcher not configured", http.StatusNotImplemented)
		return
	}

	account := chi.URLParam(r, "account")
	layer := chi.URLParam(r, "layer")
	versionStr := chi.URLParam(r, "version")

	var version int64
	if _, err := fmt.Sscanf(versionStr, "%d", &version); err != nil {
		http.Error(w, "invalid version number", http.StatusBadRequest)
		return
	}

	data, err := fetcher.GetLayerCodeZip(r.Context(), account, layer, version)
	if err != nil {
		http.Error(w, fmt.Sprintf("layer code not found: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
