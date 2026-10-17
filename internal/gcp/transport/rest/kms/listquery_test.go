package kms

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// listItemNames extracts the "name" field of each item in a list response body.
func listItemNames(t *testing.T, resp *model.ProviderResponse, collection string) []string {
	t.Helper()
	raw, ok := resp.Data[collection].([]any)
	if !ok {
		t.Fatalf("%s is not a list: %#v", collection, resp.Data[collection])
	}
	names := make([]string, 0, len(raw))
	for _, it := range raw {
		m, _ := it.(map[string]any)
		n, _ := m["name"].(string)
		names = append(names, n)
	}
	return names
}

// createRings creates key rings under location global.
func createRings(t *testing.T, p *Provider, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := p.KeyRingCreate(context.Background(), newNR(map[string]any{"location": "global", "keyRingId": id})); err != nil {
			t.Fatalf("create key ring %s: %v", id, err)
		}
	}
}

// TestKeyRingListFilterOrderBy verifies keyRings.list honors the documented
// AIP-160 `filter` (over name) and `name`/`name desc` `orderBy`, and that the
// matched count reaches totalSize.
func TestKeyRingListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr-a", "kr-b", "cloud-kr")

	// Substring filter on the full resource name.
	resp, err := p.KeyRingList(ctx, newNR(map[string]any{
		"location": "global", "filter": `name:"cloud"`,
	}))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	names := listItemNames(t, resp, "keyRings")
	if len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/cloud-kr" {
		t.Fatalf("filter name:\"cloud\" = %v, want only cloud-kr", names)
	}
	if resp.Data["totalSize"] != 1 {
		t.Errorf("totalSize = %v, want the matched count 1", resp.Data["totalSize"])
	}

	// Exact filter.
	resp, err = p.KeyRingList(ctx, newNR(map[string]any{
		"location": "global", "filter": `name="projects/proj/locations/global/keyRings/kr-a"`,
	}))
	if err != nil {
		t.Fatalf("exact-filter list: %v", err)
	}
	if names := listItemNames(t, resp, "keyRings"); len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/kr-a" {
		t.Fatalf("exact filter = %v, want only kr-a", names)
	}

	// orderBy name desc.
	resp, err = p.KeyRingList(ctx, newNR(map[string]any{"location": "global", "orderBy": "name desc"}))
	if err != nil {
		t.Fatalf("orderBy list: %v", err)
	}
	got := listItemNames(t, resp, "keyRings")
	want := []string{
		"projects/proj/locations/global/keyRings/kr-b",
		"projects/proj/locations/global/keyRings/kr-a",
		"projects/proj/locations/global/keyRings/cloud-kr",
	}
	if len(got) != len(want) {
		t.Fatalf("orderBy name desc = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orderBy name desc = %v, want %v", got, want)
		}
	}
}

// TestKeyRingListFilterComposesWithPaging verifies a filtered, ordered list
// pages without dropping or duplicating a match across the page boundary.
func TestKeyRingListFilterComposesWithPaging(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr-a", "kr-b", "kr-c", "cloud-a", "cloud-b")

	var seen []string
	token := ""
	for i := 0; i < 10; i++ {
		params := map[string]any{"location": "global", "filter": `name:"cloud"`, "orderBy": "name", "pageSize": "1"}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.KeyRingList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		seen = append(seen, listItemNames(t, resp, "keyRings")...)
		token, _ = resp.Data["nextPageToken"].(string)
		if token == "" {
			break
		}
	}
	want := []string{
		"projects/proj/locations/global/keyRings/cloud-a",
		"projects/proj/locations/global/keyRings/cloud-b",
	}
	if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("paged filtered walk = %v, want %v", seen, want)
	}
}

// TestCryptoKeyListFilterLabels verifies cryptoKeys.list filters on labels and
// purpose, and reports the matched count.
func TestCryptoKeyListFilterLabels(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	create := func(id, team string) {
		t.Helper()
		if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name":        "locations/global/keyRings/kr",
			"cryptoKeyId": id,
			"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT", "labels": map[string]any{"team": team}},
		})); err != nil {
			t.Fatalf("create crypto key %s: %v", id, err)
		}
	}
	create("key-a", "alpha")
	create("key-b", "beta")

	resp, err := p.CryptoKeyList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys", "filter": `labels.team=alpha`,
	}))
	if err != nil {
		t.Fatalf("label-filtered list: %v", err)
	}
	names := listItemNames(t, resp, "cryptoKeys")
	if len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/kr/cryptoKeys/key-a" {
		t.Fatalf("labels.team=alpha = %v, want only key-a", names)
	}
	if resp.Data["totalSize"] != 1 {
		t.Errorf("totalSize = %v, want 1", resp.Data["totalSize"])
	}

	resp, err = p.CryptoKeyList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys", "filter": `purpose=ENCRYPT_DECRYPT`,
	}))
	if err != nil {
		t.Fatalf("purpose-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "cryptoKeys"); len(names) != 2 {
		t.Fatalf("purpose=ENCRYPT_DECRYPT = %v, want both keys", names)
	}
}

// TestCryptoKeyVersionListFilterOrderBy verifies cryptoKeyVersions.list honors
// state filter and numeric `name` orderBy.
func TestCryptoKeyVersionListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "k",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("create key: %v", err)
	}
	parent := "locations/global/keyRings/kr/cryptoKeys/k"
	for i := 0; i < 11; i++ { // version 1 already exists, add 2..12
		if _, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{"name": parent})); err != nil {
			t.Fatalf("create version: %v", err)
		}
	}

	resp, err := p.CryptoKeyVersionList(ctx, newNR(map[string]any{
		"name": parent + "/cryptoKeyVersions", "orderBy": "name desc", "pageSize": "20",
	}))
	if err != nil {
		t.Fatalf("version list: %v", err)
	}
	names := listItemNames(t, resp, "cryptoKeyVersions")
	if len(names) != 12 {
		t.Fatalf("got %d versions, want 12", len(names))
	}
	// Numeric descending: version 12 first, version 1 last.
	full := "projects/proj/locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/"
	if names[0] != full+"12" || names[len(names)-1] != full+"1" {
		t.Fatalf("orderBy name desc = %v, want numeric 12..1", names)
	}

	// A state filter that matches nothing returns an empty, non-error page.
	resp, err = p.CryptoKeyVersionList(ctx, newNR(map[string]any{
		"name": parent + "/cryptoKeyVersions", "filter": `state=DESTROYED`,
	}))
	if err != nil {
		t.Fatalf("state-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "cryptoKeyVersions"); len(names) != 0 {
		t.Fatalf("state=DESTROYED = %v, want none", names)
	}
	if resp.Data["totalSize"] != 0 {
		t.Errorf("filtered totalSize = %v, want 0", resp.Data["totalSize"])
	}

	// A software key's version has an empty stored ProtectionLevel, which the
	// API renders as SOFTWARE; the filter must normalize it the same way.
	resp, err = p.CryptoKeyVersionList(ctx, newNR(map[string]any{
		"name": parent + "/cryptoKeyVersions", "filter": `protectionLevel=SOFTWARE`, "pageSize": "20",
	}))
	if err != nil {
		t.Fatalf("protectionLevel-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "cryptoKeyVersions"); len(names) != 12 {
		t.Fatalf("protectionLevel=SOFTWARE = %d versions, want all 12", len(names))
	}
}

// TestKMSListRejectsBadFilterOrderBy verifies an unsupported filter clause or
// orderBy field is INVALID_ARGUMENT (400), never a silently ignored parameter.
func TestKMSListRejectsBadFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")

	cases := []struct {
		name   string
		params map[string]any
	}{
		{"or-filter", map[string]any{"location": "global", "filter": `name:"a" OR name:"b"`}},
		{"unknown-field", map[string]any{"location": "global", "filter": `bogus=1`}},
		{"bad-operator", map[string]any{"location": "global", "filter": `name<"a"`}},
		{"unknown-order", map[string]any{"location": "global", "orderBy": "createTime"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.KeyRingList(ctx, newNR(tc.params))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			pe, ok := err.(*model.ProviderError)
			if !ok || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
				t.Fatalf("err = %#v, want InvalidArgument/400", err)
			}
		})
	}
}
