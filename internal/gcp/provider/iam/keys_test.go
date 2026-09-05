package iam

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"

	"jaiscloud/internal/store"
)

func TestServiceAccountKeysAndSign(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"

	// Create a key.
	kr, err := p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("key create: %v", err)
	}
	keyName, _ := kr.Data["name"].(string)
	if !strings.Contains(keyName, "/keys/") {
		t.Fatalf("expected key name with /keys/, got %q", keyName)
	}
	if _, ok := kr.Data["privateKeyData"]; !ok {
		t.Error("expected privateKeyData on key create")
	}
	// publicKeyData is a base64 PKIX public-key PEM derived from the private key.
	pubData, _ := kr.Data["publicKeyData"].(string)
	if pubData == "" {
		t.Error("expected publicKeyData on key create")
	} else {
		pubPEM, err := base64.StdEncoding.DecodeString(pubData)
		if err != nil {
			t.Fatalf("publicKeyData is not base64: %v", err)
		}
		block, _ := pem.Decode(pubPEM)
		if block == nil || block.Type != "PUBLIC KEY" {
			t.Fatalf("publicKeyData is not a PUBLIC KEY PEM block")
		}
		if _, err := x509.ParsePKIXPublicKey(block.Bytes); err != nil {
			t.Fatalf("publicKeyData does not parse as PKIX: %v", err)
		}
	}

	// signBlob.
	payload := base64.StdEncoding.EncodeToString([]byte("hello"))
	sr, err := p.ServiceAccountSignBlob(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"payload": payload}}))
	if err != nil {
		t.Fatalf("signBlob: %v", err)
	}
	keyID, _ := sr.Data["keyId"].(string)
	sig, _ := base64.StdEncoding.DecodeString(sr.Data["signature"].(string))

	// Verify the signature with the stored key's public key.
	e, _ := p.resources.Get(ctx, "proj", store.GlobalRegion, rtServiceAccountKey, keyID)
	var m serviceAccountKeyMeta
	json.Unmarshal(e.Data, &m)
	privDER, _ := m.privDER()
	priv, _ := x509.ParsePKCS8PrivateKey(privDER)
	digest := sha256.Sum256([]byte("hello"))
	if err := rsa.VerifyPKCS1v15(priv.(*rsa.PrivateKey).Public().(*rsa.PublicKey), crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("verify signature: %v", err)
	}

	// signJwt.
	jr, err := p.ServiceAccountSignJwt(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"payload": `{"iss":"proj"}`}}))
	if err != nil {
		t.Fatalf("signJwt: %v", err)
	}
	signedJwt, _ := jr.Data["signedJwt"].(string)
	if got := strings.Count(signedJwt, "."); got != 2 {
		t.Fatalf("expected 3-part JWT, got %d dots", got)
	}

	// List keys → 1.
	lr, err := p.ServiceAccountKeyList(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("key list: %v", err)
	}
	if keys, _ := lr.Data["keys"].([]any); len(keys) != 1 {
		t.Fatalf("expected 1 key, got %v", lr.Data["keys"])
	}

	// Delete the key.
	if _, err := p.ServiceAccountKeyDelete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("key delete: %v", err)
	}
	if _, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err == nil {
		t.Fatal("expected 404 on deleted key")
	}
}
