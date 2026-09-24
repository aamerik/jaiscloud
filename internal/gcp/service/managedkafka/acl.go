package managedkafka

import (
	"context"
	"errors"
	"strings"

	"jaiscloud/internal/gcp/paging"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// AclEntryInput carries the caller-supplied fields of one ACL entry.
type AclEntryInput struct {
	Principal      string
	PermissionType string
	Operation      string
	Host           string
}

// AclInput carries the caller-supplied fields of an ACL create/update. Etag is
// only meaningful for UpdateAcl, where it backs the optimistic-concurrency
// check.
type AclInput struct {
	AclEntries []AclEntryInput
	Etag       string
}

// deriveAclPattern maps an acl id to the output-only (resourceType,
// resourceName, patternType) triple the API derives from it. An unrecognised id
// is InvalidArgument.
func deriveAclPattern(aclID string) (resourceType, resourceName, patternType string, err error) {
	switch aclID {
	case "cluster":
		return "CLUSTER", "kafka-cluster", "LITERAL", nil
	case "allTopics":
		return "TOPIC", "*", "LITERAL", nil
	case "allConsumerGroups":
		return "GROUP", "*", "LITERAL", nil
	case "allTransactionalIds":
		return "TRANSACTIONAL_ID", "*", "LITERAL", nil
	}
	type pattern struct {
		prefix      string
		resource    string
		patternType string
	}
	for _, p := range []pattern{
		{"topic/", "TOPIC", "LITERAL"},
		{"topicPrefixed/", "TOPIC", "PREFIXED"},
		{"consumerGroup/", "GROUP", "LITERAL"},
		{"consumerGroupPrefixed/", "GROUP", "PREFIXED"},
		{"transactionalId/", "TRANSACTIONAL_ID", "LITERAL"},
		{"transactionalIdPrefixed/", "TRANSACTIONAL_ID", "PREFIXED"},
	} {
		if name, ok := strings.CutPrefix(aclID, p.prefix); ok && name != "" {
			return p.resource, name, p.patternType, nil
		}
	}
	return "", "", "", invalidArgument("invalid acl id " + aclID)
}

// toEntries converts typed input entries to stored entries.
func toEntries(in []AclEntryInput) []mkstore.AclEntry {
	out := make([]mkstore.AclEntry, 0, len(in))
	for _, e := range in {
		out = append(out, mkstore.AclEntry{
			Principal:      e.Principal,
			PermissionType: e.PermissionType,
			Operation:      e.Operation,
			Host:           e.Host,
		})
	}
	return out
}

// CreateAcl creates an ACL for an existing cluster.
func (s *Service) CreateAcl(ctx context.Context, project, location, clusterID, aclID string, in AclInput) (mkstore.Acl, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, invalidArgument("missing location, clusterId, or aclId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	if len(in.AclEntries) == 0 {
		return mkstore.Acl{}, invalidArgument("aclEntries must not be empty")
	}
	resourceType, resourceName, patternType, err := deriveAclPattern(aclID)
	if err != nil {
		return mkstore.Acl{}, err
	}
	a := mkstore.Acl{
		Location:     location,
		ClusterName:  clusterID,
		Name:         aclID,
		AclEntries:   toEntries(in.AclEntries),
		Etag:         randomHex(24),
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
	}
	if err := s.store.CreateAcl(ctx, project, location, clusterID, a); err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	return a, nil
}

// GetAcl returns one ACL.
func (s *Service) GetAcl(ctx context.Context, project, location, clusterID, aclID string) (mkstore.Acl, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, invalidArgument("missing location, clusterId, or aclId")
	}
	a, err := s.store.GetAcl(ctx, project, location, clusterID, aclID)
	if err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	return a, nil
}

// ListAcls returns a cursor page of the ACLs in a cluster.
func (s *Service) ListAcls(ctx context.Context, project, location, clusterID string, pageSize int, pageToken string) ([]mkstore.Acl, string, error) {
	if location == "" || clusterID == "" {
		return nil, "", invalidArgument("missing location or clusterId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return nil, "", mapStoreError(err)
	}
	acls, err := s.store.ListAcls(ctx, project, location, clusterID)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(acls, func(a mkstore.Acl) string { return a.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateAcl replaces an ACL's entries under the etag optimistic-concurrency
// check. A missing/stale etag is ABORTED, matching real GCP.
func (s *Service) UpdateAcl(ctx context.Context, project, location, clusterID, aclID string, in AclInput) (mkstore.Acl, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, invalidArgument("missing location, clusterId, or aclId")
	}
	if in.Etag == "" {
		return mkstore.Acl{}, invalidArgument("missing etag")
	}
	if len(in.AclEntries) == 0 {
		return mkstore.Acl{}, invalidArgument("aclEntries must not be empty; use DeleteAcl to remove an acl")
	}
	a, err := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		if cur.Etag != in.Etag {
			return mkstore.Acl{}, etagMismatch()
		}
		cur.AclEntries = toEntries(in.AclEntries)
		cur.Etag = randomHex(24)
		return cur, nil
	})
	if err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	return a, nil
}

// DeleteAcl deletes an ACL.
func (s *Service) DeleteAcl(ctx context.Context, project, location, clusterID, aclID string) error {
	if location == "" || clusterID == "" || aclID == "" {
		return invalidArgument("missing location, clusterId, or aclId")
	}
	if err := s.store.DeleteAcl(ctx, project, location, clusterID, aclID); err != nil {
		return mapStoreError(err)
	}
	return nil
}

// AddAclEntry adds an entry to an ACL, creating the ACL when it does not exist
// yet. It returns the updated (or created) ACL and whether the ACL was created.
// Adding an entry that already exists is a no-op.
func (s *Service) AddAclEntry(ctx context.Context, project, location, clusterID, aclID string, entry AclEntryInput) (mkstore.Acl, bool, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, false, invalidArgument("missing location, clusterId, or aclId")
	}
	created := false
	a, err := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		if !aclHasEntry(cur.AclEntries, entry) {
			cur.AclEntries = append(cur.AclEntries, toEntry(entry))
			cur.Etag = randomHex(24)
		}
		return cur, nil
	})
	if err == nil {
		return a, created, nil
	}
	if !errors.Is(err, mkstore.ErrNoSuchAcl) {
		return mkstore.Acl{}, false, mapStoreError(err)
	}
	// The acl does not exist yet: create it with the entry. If a concurrent
	// AddAclEntry created it first, fall back to appending to the winner.
	created = true
	res, err := s.CreateAcl(ctx, project, location, clusterID, aclID, AclInput{AclEntries: []AclEntryInput{entry}})
	if err == nil {
		return res, created, nil
	}
	if !isAlreadyExists(err) {
		return mkstore.Acl{}, false, err
	}
	a, err = s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		if !aclHasEntry(cur.AclEntries, entry) {
			cur.AclEntries = append(cur.AclEntries, toEntry(entry))
			cur.Etag = randomHex(24)
		}
		return cur, nil
	})
	if err != nil {
		return mkstore.Acl{}, false, mapStoreError(err)
	}
	return a, false, nil
}

// isAlreadyExists reports whether err is an AlreadyExists provider error.
func isAlreadyExists(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "AlreadyExists"
}

// RemoveAclEntry removes an entry from an ACL. If it was the last entry the ACL
// is deleted and deleted reports true; otherwise the updated ACL is returned.
// Removing an absent entry is a no-op.
func (s *Service) RemoveAclEntry(ctx context.Context, project, location, clusterID, aclID string, entry AclEntryInput) (*mkstore.Acl, bool, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return nil, false, invalidArgument("missing location, clusterId, or aclId")
	}
	deleted := false
	a, err := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		kept := make([]mkstore.AclEntry, 0, len(cur.AclEntries))
		for _, e := range cur.AclEntries {
			if sameEntry(e, entry) {
				continue
			}
			kept = append(kept, e)
		}
		if len(kept) == 0 {
			deleted = true
		}
		if len(kept) != len(cur.AclEntries) {
			cur.Etag = randomHex(24)
		}
		cur.AclEntries = kept
		return cur, nil
	})
	if err != nil {
		return nil, false, mapStoreError(err)
	}
	if deleted {
		if err := s.store.DeleteAcl(ctx, project, location, clusterID, aclID); err != nil {
			return nil, false, mapStoreError(err)
		}
		return nil, true, nil
	}
	return &a, false, nil
}

func toEntry(e AclEntryInput) mkstore.AclEntry {
	return mkstore.AclEntry{
		Principal:      e.Principal,
		PermissionType: e.PermissionType,
		Operation:      e.Operation,
		Host:           e.Host,
	}
}

func aclHasEntry(entries []mkstore.AclEntry, e AclEntryInput) bool {
	for _, cur := range entries {
		if sameEntry(cur, e) {
			return true
		}
	}
	return false
}

func sameEntry(a mkstore.AclEntry, b AclEntryInput) bool {
	return a.Principal == b.Principal &&
		a.PermissionType == b.PermissionType &&
		a.Operation == b.Operation &&
		a.Host == b.Host
}

// etagMismatch builds the ABORTED error for a stale optimistic-concurrency
// token.
func etagMismatch() error {
	return model.NewProviderError("Aborted", "etag mismatch", 409)
}
