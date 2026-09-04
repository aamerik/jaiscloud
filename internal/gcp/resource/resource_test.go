package resource

import "testing"

func TestResourceIDFormatters(t *testing.T) {
	r := ResourceID("proj")
	cases := []struct {
		typ, name, want string
	}{
		{"gcs-bucket", "bkt", "bkt"},
		{"gcs-object", "obj", "obj"},
		{"gcs-bucket-policy", "bkt", "projects/_/buckets/bkt"},
		{"gcs-object-policy", "bkt/objects/o", "projects/_/buckets/bkt/objects/o"},
		{"pubsub-topic", "t", "projects/proj/topics/t"},
		{"pubsub-subscription", "s", "projects/proj/subscriptions/s"},
		{"secret", "sec", "projects/proj/secrets/sec"},
		{"kms-keyring", "us/keyring1", "projects/proj/locations/us/keyRings/keyring1"},
		{"kms-cryptokey", "us/keyring1/key1", "projects/proj/locations/us/keyRings/keyring1/cryptoKeys/key1"},
		{"kms-cryptokey-version", "us/keyring1/key1/3", "projects/proj/locations/us/keyRings/keyring1/cryptoKeys/key1/cryptoKeyVersions/3"},
		{"service-account", "sa@x.iam.gserviceaccount.com", "projects/proj/serviceAccounts/sa@x.iam.gserviceaccount.com"},
		{"cloud-function", "fn", "projects/proj/locations/-/functions/fn"},
	}
	for _, tc := range cases {
		if got := r(tc.typ, tc.name); got != tc.want {
			t.Errorf("ResourceID(%q, %q) = %q, want %q", tc.typ, tc.name, got, tc.want)
		}
	}

	// Unknown types fall back to the name as-is.
	if got := r("unknown-type", "x"); got != "x" {
		t.Errorf("unknown type = %q, want %q", got, "x")
	}
}

func TestLocRingKeyOf(t *testing.T) {
	if got := locOf("us/keyring1/key1"); got != "us" {
		t.Errorf("locOf = %q, want us", got)
	}
	if got := locOf("no-slash"); got != "global" {
		t.Errorf("locOf(no-slash) = %q, want global", got)
	}
	if got := ringOf("us/keyring1/key1"); got != "keyring1" {
		t.Errorf("ringOf = %q, want keyring1", got)
	}
	if got := ringOf("us/keyring1"); got != "keyring1" {
		t.Errorf("ringOf(no key) = %q, want keyring1", got)
	}
	if got := ringOf("noslash"); got != "noslash" {
		t.Errorf("ringOf(noslash) = %q, want noslash", got)
	}
	if got := keyOf("us/keyring1/key1"); got != "key1" {
		t.Errorf("keyOf = %q, want key1", got)
	}
	if got := keyOf("us/keyring1"); got != "" {
		t.Errorf("keyOf(no key) = %q, want empty", got)
	}
	if got := keyOf("noslash"); got != "" {
		t.Errorf("keyOf(noslash) = %q, want empty", got)
	}
}
