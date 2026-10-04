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
	"net/http"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
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
	// The key id is the trailing segment of name — real GCP's ServiceAccountKey
	// has no separate keyId field, so the response must not invent one.
	if _, present := kr.Data["keyId"]; present {
		t.Errorf("key create must not emit a non-schema keyId field: %v", kr.Data["keyId"])
	}
	keyIDField := keyName[strings.LastIndex(keyName, "/")+1:]
	if keyIDField == "" {
		t.Fatalf("expected a key id in the name %q", keyName)
	}
	if kr.Data["keyOrigin"] != "USER_PROVIDED" {
		t.Errorf("keyOrigin = %v, want USER_PROVIDED", kr.Data["keyOrigin"])
	}
	if kr.Data["keyType"] != "USER_MANAGED" {
		t.Errorf("keyType = %v, want USER_MANAGED", kr.Data["keyType"])
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
	privDER, _ := m.PrivDER()
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
	keys, _ := lr.Data["keys"].([]any)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %v", lr.Data["keys"])
	}
	listed, _ := keys[0].(map[string]any)
	if listedName, _ := listed["name"].(string); !strings.HasSuffix(listedName, "/keys/"+keyIDField) {
		t.Errorf("listed key name = %v, want suffix /keys/%s (stable across list)", listedName, keyIDField)
	}
	if _, present := listed["keyId"]; present {
		t.Errorf("keys.list must not emit a non-schema keyId field: %v", listed["keyId"])
	}

	// Delete the key.
	if _, err := p.ServiceAccountKeyDelete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("key delete: %v", err)
	}
	if _, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err == nil {
		t.Fatal("expected 404 on deleted key")
	}
}

// TestServiceAccountSignBlobResponseFieldByHost covers J64: the IAM and IAM
// Credentials APIs share one path on the emulator origin but return different
// signature field names. The request Host selects exactly one — never both.
func TestServiceAccountSignBlobResponseFieldByHost(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"
	payload := base64.StdEncoding.EncodeToString([]byte("hello"))

	sign := func(host string) map[string]any {
		t.Helper()
		nr := newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"payload": payload}})
		nr.Raw = &http.Request{Host: host}
		resp, err := p.ServiceAccountSignBlob(ctx, nr)
		if err != nil {
			t.Fatalf("signBlob(host=%q): %v", host, err)
		}
		return resp.Data
	}

	iam := sign("iam.googleapis.com")
	if _, ok := iam["signature"]; !ok {
		t.Errorf("IAM signBlob must return signature: %v", iam)
	}
	if _, ok := iam["signedBlob"]; ok {
		t.Errorf("IAM signBlob must not return the IAM Credentials field signedBlob: %v", iam)
	}

	creds := sign("iamcredentials.googleapis.com")
	if _, ok := creds["signedBlob"]; !ok {
		t.Errorf("IAM Credentials signBlob must return signedBlob: %v", creds)
	}
	if _, ok := creds["signature"]; ok {
		t.Errorf("IAM Credentials signBlob must not return the IAM field signature: %v", creds)
	}

	// An unqualified host (a bare emulator endpoint) defaults to the IAM spelling.
	if local := sign("localhost:8080"); local["signature"] == nil || local["signedBlob"] != nil {
		t.Errorf("default-host signBlob must return signature only: %v", local)
	}
}

// TestServiceAccountSignValidation covers the shared iamcredentials validation
// applied to the IAM signBlob/signJwt wire methods: empty blobs and
// out-of-bounds JWT claims fail loud, and the projects/- wildcard signs lazily.
func TestServiceAccountSignValidation(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	email := "sa@proj.iam.gserviceaccount.com"
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}

	badRequest := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected an error", name)
		}
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 {
			t.Fatalf("%s: error = %v, want 400 ProviderError", name, err)
		}
	}

	// Empty blob.
	_, err := p.ServiceAccountSignBlob(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{}}))
	badRequest("empty signBlob", err)

	// JWT payload that is not JSON, and one with an exp in the past.
	if _, err := p.ServiceAccountSignJwt(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"payload": "nope"}})); err == nil {
		t.Fatal("non-JSON signJwt: expected an error")
	}
	_, err = p.ServiceAccountSignJwt(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"payload": `{"exp":1}`}}))
	badRequest("past-exp signJwt", err)

	// The projects/- wildcard signs lazily for an account that does not exist.
	wild := &model.NormalizedRequest{
		AccountID:  "-",
		Params:     map[string]any{"name": "serviceAccounts/ghost@proj.iam.gserviceaccount.com", "body": map[string]any{"payload": base64.StdEncoding.EncodeToString([]byte("hi"))}},
		ResourceID: resource.ResourceID("proj"),
	}
	if _, err := p.ServiceAccountSignBlob(ctx, wild); err != nil {
		t.Fatalf("wildcard signBlob: %v", err)
	}
	wild.Params["body"] = map[string]any{"payload": `{"iss":"proj"}`}
	if _, err := p.ServiceAccountSignJwt(ctx, wild); err != nil {
		t.Fatalf("wildcard signJwt: %v", err)
	}
}
