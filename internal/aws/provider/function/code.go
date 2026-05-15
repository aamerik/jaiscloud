package function

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func blobKeyFor(account, funcName, versionID string) string {
	return fmt.Sprintf("lambda/%s/%s/%s/code.zip", account, funcName, versionID)
}

func layerBlobKey(account, layerName string, version int64) string {
	return fmt.Sprintf("lambda/layer/%s/%s/%d.zip", account, layerName, version)
}

func (p *FunctionProvider) storeCode(ctx context.Context, account, funcName, versionID string, zipBytes []byte) (sha256hex string, codeSize int64, blobKey string, err error) {
	sum := sha256.Sum256(zipBytes)
	sha256hex = hex.EncodeToString(sum[:])
	codeSize = int64(len(zipBytes))
	blobKey = blobKeyFor(account, funcName, versionID)
	err = p.blobs.Put(ctx, "lambda-code", blobKey, zipBytes)
	return
}

func (p *FunctionProvider) loadCode(ctx context.Context, account, funcName, versionID string) ([]byte, error) {
	key := blobKeyFor(account, funcName, versionID)
	data, err := p.blobs.Get(ctx, "lambda-code", key)
	if err != nil {
		return nil, fmt.Errorf("lambda: load code %s/%s/%s: %w", account, funcName, versionID, err)
	}
	return data, nil
}

func (p *FunctionProvider) storeLayerCode(ctx context.Context, account, layerName string, version int64, zipBytes []byte) (string, int64, string, error) {
	sum := sha256.Sum256(zipBytes)
	sha256hex := hex.EncodeToString(sum[:])
	codeSize := int64(len(zipBytes))
	key := layerBlobKey(account, layerName, version)
	err := p.blobs.Put(ctx, "lambda-code", key, zipBytes)
	return sha256hex, codeSize, key, err
}

func (p *FunctionProvider) loadLayerCode(ctx context.Context, account, layerName string, version int64) ([]byte, error) {
	key := layerBlobKey(account, layerName, version)
	data, err := p.blobs.Get(ctx, "lambda-code", key)
	if err != nil {
		return nil, fmt.Errorf("lambda: load layer code %s/%s/%d: %w", account, layerName, version, err)
	}
	return data, nil
}

// LoadCode implements the CodeLoader interface for executor injection.
func (p *FunctionProvider) LoadCode(ctx context.Context, account, funcName, version string) ([]byte, error) {
	return p.loadCode(ctx, account, funcName, version)
}

