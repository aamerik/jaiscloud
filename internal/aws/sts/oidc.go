package sts

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// JWKSCache caches JWKS keys per issuer with a 1-hour TTL.
type JWKSCache struct {
	mu      sync.RWMutex
	entries map[string]jwksCacheEntry // keyed by issuer
}

// NewJWKSCache creates a new empty JWKS cache.
func NewJWKSCache() *JWKSCache {
	return &JWKSCache{entries: make(map[string]jwksCacheEntry)}
}

type jwksCacheEntry struct {
	keys      []jwk
	fetchedAt time.Time
}

type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwksResponse struct {
	Keys []jwk `json:"keys"`
}

const jwksTTL = time.Hour

// verifyJWT parses the JWT, fetches the JWKS, and verifies the RS256 signature.
// Returns the claims payload on success.
func (c *JWKSCache) verifyJWT(tokenStr string, trustedIssuers map[string]string) (map[string]any, error) {
	// 1. Split JWT into header.payload.signature
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format: expected 3 parts, got %d", len(parts))
	}

	// 2. Decode header to get "kid" and "alg"
	headerJSON, err := base64URLDecode(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, fmt.Errorf("invalid JWT header JSON: %w", err)
	}
	if header.Alg != "RS256" {
		return nil, fmt.Errorf("unsupported JWT algorithm %q: only RS256 is supported", header.Alg)
	}

	// 3. Decode payload to get claims
	payloadJSON, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT payload: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, fmt.Errorf("invalid JWT payload JSON: %w", err)
	}

	// 4. Extract issuer and look it up in trusted issuers
	iss, _ := claims["iss"].(string)
	if iss == "" {
		return nil, fmt.Errorf("JWT missing 'iss' claim")
	}
	jwksURL, ok := trustedIssuers[iss]
	if !ok {
		return nil, fmt.Errorf("untrusted issuer %q", iss)
	}

	// 5. Check expiry
	if expRaw, ok := claims["exp"]; ok {
		var expUnix int64
		switch v := expRaw.(type) {
		case float64:
			expUnix = int64(v)
		case json.Number:
			expUnix, _ = v.Int64()
		}
		if expUnix > 0 && expUnix < time.Now().Unix() {
			return nil, fmt.Errorf("JWT has expired")
		}
	}

	// 6. Fetch JWKS (with cache)
	keys, err := c.fetchKeys(iss, jwksURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS for issuer %q: %w", iss, err)
	}

	// 7. Find key matching kid
	var matchedKey *jwk
	for i := range keys {
		if header.Kid == "" || keys[i].Kid == header.Kid {
			matchedKey = &keys[i]
			break
		}
	}
	if matchedKey == nil {
		return nil, fmt.Errorf("no matching JWK found for kid=%q", header.Kid)
	}

	// 8. Verify RS256 signature
	signingInput := parts[0] + "." + parts[1]
	sigBytes, err := base64URLDecodeBytes(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid JWT signature encoding: %w", err)
	}
	pubKey, err := jwkToRSAPublicKey(matchedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWK public key: %w", err)
	}
	h := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, h[:], sigBytes); err != nil {
		return nil, fmt.Errorf("JWT signature verification failed: %w", err)
	}

	return claims, nil
}

// fetchKeys returns the JWKS keys for the given issuer URL, using cache if available.
func (c *JWKSCache) fetchKeys(issuer, jwksURL string) ([]jwk, error) {
	c.mu.RLock()
	entry, ok := c.entries[issuer]
	c.mu.RUnlock()
	if ok && time.Since(entry.fetchedAt) < jwksTTL {
		return entry.keys, nil
	}

	// Fetch fresh keys
	resp, err := http.Get(jwksURL) //nolint:gosec
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var jwks jwksResponse
	if err := json.Unmarshal(body, &jwks); err != nil {
		return nil, fmt.Errorf("invalid JWKS response: %w", err)
	}

	c.mu.Lock()
	c.entries[issuer] = jwksCacheEntry{keys: jwks.Keys, fetchedAt: time.Now()}
	c.mu.Unlock()

	return jwks.Keys, nil
}

// base64URLDecode decodes a base64url-encoded string (with or without padding).
func base64URLDecode(s string) ([]byte, error) {
	// Add padding
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.URLEncoding.DecodeString(s)
}

func base64URLDecodeBytes(s string) ([]byte, error) {
	return base64URLDecode(s)
}

// jwkToRSAPublicKey converts a JWK (RSA) to an *rsa.PublicKey.
func jwkToRSAPublicKey(k *jwk) (*rsa.PublicKey, error) {
	if k.Kty != "RSA" {
		return nil, fmt.Errorf("unsupported key type %q", k.Kty)
	}
	nBytes, err := base64URLDecodeBytes(k.N)
	if err != nil {
		return nil, fmt.Errorf("invalid JWK 'n': %w", err)
	}
	eBytes, err := base64URLDecodeBytes(k.E)
	if err != nil {
		return nil, fmt.Errorf("invalid JWK 'e': %w", err)
	}
	n := new(big.Int).SetBytes(nBytes)
	eInt := 0
	for _, b := range eBytes {
		eInt = eInt<<8 | int(b)
	}
	return &rsa.PublicKey{N: n, E: eInt}, nil
}
