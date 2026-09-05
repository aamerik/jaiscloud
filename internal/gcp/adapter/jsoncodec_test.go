package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestJSONCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		// Secret Manager
		{"POST", "/v1/projects/p/secrets?secretId=s", "Create"},
		{"GET", "/v1/projects/p/secrets", "List"},
		{"GET", "/v1/projects/p/secrets/s", "Get"},
		{"PATCH", "/v1/projects/p/secrets/s", "Update"},
		{"DELETE", "/v1/projects/p/secrets/s", "Delete"},
		{"POST", "/v1/projects/p/secrets/s:addVersion", "AddVersion"},
		{"POST", "/v1/projects/p/secrets/s/versions/1:access", "Access"},
		{"GET", "/v1/projects/p/secrets/s/versions/1", "GetVersion"},
		{"GET", "/v1/projects/p/secrets/s:getIamPolicy", "SecretGetIamPolicy"},
		{"POST", "/v1/projects/p/secrets/s:setIamPolicy", "SecretSetIamPolicy"},
		{"POST", "/v1/projects/p/secrets/s:testIamPermissions", "SecretTestIamPermissions"},
		// Pub/Sub
		{"PUT", "/v1/projects/p/topics/t", "TopicCreate"},
		{"GET", "/v1/projects/p/topics", "TopicList"},
		{"GET", "/v1/projects/p/topics/t", "TopicGet"},
		{"DELETE", "/v1/projects/p/topics/t", "TopicDelete"},
		{"POST", "/v1/projects/p/topics/t:publish", "TopicPublish"},
		{"GET", "/v1/projects/p/topics/t:getIamPolicy", "TopicGetIamPolicy"},
		{"POST", "/v1/projects/p/topics/t:setIamPolicy", "TopicSetIamPolicy"},
		{"POST", "/v1/projects/p/topics/t:testIamPermissions", "TopicTestIamPermissions"},
		{"PUT", "/v1/projects/p/subscriptions/s", "SubscriptionCreate"},
		{"GET", "/v1/projects/p/subscriptions", "SubscriptionList"},
		{"POST", "/v1/projects/p/subscriptions/s:pull", "SubscriptionPull"},
		{"POST", "/v1/projects/p/subscriptions/s:acknowledge", "SubscriptionAcknowledge"},
		{"GET", "/v1/projects/p/subscriptions/s:getIamPolicy", "SubscriptionGetIamPolicy"},
		{"POST", "/v1/projects/p/subscriptions/s:setIamPolicy", "SubscriptionSetIamPolicy"},
		{"POST", "/v1/projects/p/subscriptions/s:testIamPermissions", "SubscriptionTestIamPermissions"},
		// KMS
		{"POST", "/v1/projects/p/locations/us/keyRings?keyRingId=kr", "KeyRingCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings", "KeyRingList"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr", "KeyRingGet"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys?cryptoKeyId=k", "CryptoKeyCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys", "CryptoKeyList"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:encrypt", "CryptoKeyEncrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:decrypt", "CryptoKeyDecrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:updatePrimaryVersion", "CryptoKeyUpdatePrimaryVersion"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions", "CryptoKeyVersionCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions", "CryptoKeyVersionList"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3", "CryptoKeyVersionGet"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:destroy", "CryptoKeyVersionDestroy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:disable", "CryptoKeyVersionDisable"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:enable", "CryptoKeyVersionEnable"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:asymmetricSign", "CryptoKeyVersionAsymmetricSign"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:asymmetricDecrypt", "CryptoKeyVersionAsymmetricDecrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:macSign", "CryptoKeyVersionMacSign"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:macVerify", "CryptoKeyVersionMacVerify"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3/publicKey", "CryptoKeyVersionGetPublicKey"},
		// IAM
		{"POST", "/v1/projects/p/serviceAccounts", "ServiceAccountCreate"},
		{"GET", "/v1/projects/p/serviceAccounts", "ServiceAccountList"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountGet"},
		{"DELETE", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountDelete"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com:getIamPolicy", "ServiceAccountGetIamPolicy"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:setIamPolicy", "ServiceAccountSetIamPolicy"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:testIamPermissions", "ServiceAccountTestIamPermissions"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:signBlob", "ServiceAccountSignBlob"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:signJwt", "ServiceAccountSignJwt"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com/keys", "ServiceAccountKeyCreate"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com/keys", "ServiceAccountKeyList"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1", "ServiceAccountKeyGet"},
		{"DELETE", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1", "ServiceAccountKeyDelete"},
	}
	for _, tc := range cases {
		codec := &JSONCodec{Service: "test"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}

func TestDetectV1Service(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/topics/t":                                                  "pubsub",
		"/v1/projects/p/subscriptions/s":                                           "pubsub",
		"/v1/projects/p/secrets/s":                                                 "secretmanager",
		"/v1/projects/p/locations/us/keyRings/kr":                                  "kms",
		"/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3": "kms",
		"/v1/projects/p/serviceAccounts/sa@x.com":                                  "iam",
		"/storage/v1/b/bkt/o":                                                      "",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}
}
