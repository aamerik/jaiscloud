package ecr

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryECRStore is the default in-memory ECR store.
type MemoryECRStore struct {
	mu           sync.RWMutex
	repos        map[string]*Repository // name → repo
	arnToName    map[string]string      // ARN → name
	ptcRules     map[string]*PullThroughCacheRule // prefix → rule
	registryPolicy    string
	replicationConfig string
}

func NewMemoryECRStore() *MemoryECRStore {
	return &MemoryECRStore{
		repos:     make(map[string]*Repository),
		arnToName: make(map[string]string),
		ptcRules:  make(map[string]*PullThroughCacheRule),
	}
}

// ─── Repository CRUD ─────────────────────────────────────────────────────────

func (s *MemoryECRStore) CreateRepository(repo *Repository) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.repos[repo.Name]; exists {
		return &ECRError{Code: "RepositoryAlreadyExistsException", Message: fmt.Sprintf("The repository with name '%s' already exists in the registry with id '%s'", repo.Name, repo.RegistryID), Status: 400}
	}
	if len(s.repos) >= 10000 {
		return &ECRError{Code: "LimitExceededException", Message: "The number of repositories in this registry exceeds the limit", Status: 400}
	}
	clone := cloneRepo(repo)
	s.repos[clone.Name] = clone
	s.arnToName[clone.ARN] = clone.Name
	return nil
}

func (s *MemoryECRStore) GetRepository(name string) (*Repository, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[name]
	if !ok {
		return nil, &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", name), Status: 400}
	}
	return cloneRepo(r), nil
}

func (s *MemoryECRStore) DeleteRepository(name string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[name]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", name), Status: 400}
	}
	if !force && len(r.Images) > 0 {
		return &ECRError{Code: "RepositoryNotEmptyException", Message: fmt.Sprintf("The repository with name '%s' is not empty", name), Status: 400}
	}
	delete(s.arnToName, r.ARN)
	delete(s.repos, name)
	return nil
}

func (s *MemoryECRStore) ListRepositories(prefix string) []*Repository {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Repository
	for _, r := range s.repos {
		if prefix == "" || strings.HasPrefix(r.Name, prefix) {
			out = append(out, cloneRepo(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ─── Image Operations ─────────────────────────────────────────────────────────

func (s *MemoryECRStore) PutImage(repoName string, image *Image) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[repoName]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}

	// Check immutability for tag
	if image.Tags != nil {
		for _, tag := range image.Tags {
			for _, existing := range r.Images {
				for _, existingTag := range existing.Tags {
					if existingTag == tag && r.ImageTagMutability == "IMMUTABLE" {
						return &ECRError{Code: "ImageTagAlreadyExistsException", Message: fmt.Sprintf("The image tag '%s' already exists in the repository", tag), Status: 400}
					}
				}
			}
		}
	}

	// Remove the tag from any existing image that has it (MUTABLE mode)
	if image.Tags != nil {
		for _, tag := range image.Tags {
			for digest, existing := range r.Images {
				newTags := existing.Tags[:0]
				for _, t := range existing.Tags {
					if t != tag {
						newTags = append(newTags, t)
					}
				}
				r.Images[digest].Tags = newTags
			}
		}
	}

	// If image with same digest already exists, merge tags
	if existing, ok := r.Images[image.Digest]; ok {
		if image.Tags != nil {
			existing.Tags = mergeTags(existing.Tags, image.Tags)
		}
		return nil
	}

	clone := cloneImage(image)
	r.Images[clone.Digest] = clone
	return nil
}

func (s *MemoryECRStore) GetImage(repoName string, id ImageIdentifier) (*Image, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[repoName]
	if !ok {
		return nil, &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	img := findImage(r, id)
	if img == nil {
		return nil, &ECRError{Code: "ImageNotFoundException", Message: "The image requested does not exist in the specified repository", Status: 400}
	}
	return cloneImage(img), nil
}

func (s *MemoryECRStore) BatchGetImages(repoName string, ids []ImageIdentifier) (found []*Image, notFound []ImageIdentifier) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[repoName]
	if !ok {
		return nil, ids
	}
	for _, id := range ids {
		if img := findImage(r, id); img != nil {
			found = append(found, cloneImage(img))
		} else {
			notFound = append(notFound, id)
		}
	}
	return
}

func (s *MemoryECRStore) BatchDeleteImages(repoName string, ids []ImageIdentifier) (deleted []ImageIdentifier, failed []FailedImage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[repoName]
	if !ok {
		for _, id := range ids {
			failed = append(failed, FailedImage{ImageID: id, FailureCode: "RepositoryNotFoundException", FailureReason: "repository not found"})
		}
		return
	}
	for _, id := range ids {
		if id.ImageDigest != "" && id.ImageTag == "" {
			// Delete by digest: remove the manifest and all its tags
			img, exists := r.Images[id.ImageDigest]
			if !exists {
				failed = append(failed, FailedImage{ImageID: id, FailureCode: "ImageNotFoundException", FailureReason: "image not found"})
				continue
			}
			// Return one entry per tag plus the bare digest
			deleted = append(deleted, ImageIdentifier{ImageDigest: id.ImageDigest})
			for _, tag := range img.Tags {
				deleted = append(deleted, ImageIdentifier{ImageDigest: id.ImageDigest, ImageTag: tag})
			}
			delete(r.Images, id.ImageDigest)
		} else if id.ImageTag != "" {
			// Delete by tag: only remove the tag (manifest persists if other tags remain)
			found := false
			for digest, img := range r.Images {
				for i, t := range img.Tags {
					if t == id.ImageTag {
						found = true
						deleted = append(deleted, ImageIdentifier{ImageDigest: digest, ImageTag: id.ImageTag})
						img.Tags = append(img.Tags[:i], img.Tags[i+1:]...)
						// If no tags remain, keep manifest (can still be fetched by digest)
						break
					}
				}
				if found {
					break
				}
			}
			if !found {
				failed = append(failed, FailedImage{ImageID: id, FailureCode: "ImageNotFoundException", FailureReason: "image tag not found"})
			}
		} else {
			failed = append(failed, FailedImage{ImageID: id, FailureCode: "MissingDigestAndTag", FailureReason: "imageDigest or imageTag must be specified"})
		}
	}
	return
}

func (s *MemoryECRStore) ListImages(repoName string, tagStatus string) []ImageIdentifier {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[repoName]
	if !ok {
		return nil
	}
	var out []ImageIdentifier
	for digest, img := range r.Images {
		if len(img.Tags) == 0 {
			if tagStatus == "TAGGED" {
				continue
			}
			out = append(out, ImageIdentifier{ImageDigest: digest})
		} else {
			if tagStatus == "UNTAGGED" {
				continue
			}
			for _, tag := range img.Tags {
				out = append(out, ImageIdentifier{ImageDigest: digest, ImageTag: tag})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ImageDigest != out[j].ImageDigest {
			return out[i].ImageDigest < out[j].ImageDigest
		}
		return out[i].ImageTag < out[j].ImageTag
	})
	return out
}

func (s *MemoryECRStore) DescribeImages(repoName string, ids []ImageIdentifier) []*Image {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[repoName]
	if !ok {
		return nil
	}
	if len(ids) == 0 {
		// Return all
		var all []*Image
		for _, img := range r.Images {
			all = append(all, cloneImage(img))
		}
		return all
	}
	var out []*Image
	seen := make(map[string]bool)
	for _, id := range ids {
		img := findImage(r, id)
		if img != nil && !seen[img.Digest] {
			seen[img.Digest] = true
			out = append(out, cloneImage(img))
		}
	}
	return out
}

// ─── Policies ─────────────────────────────────────────────────────────────────

func (s *MemoryECRStore) PutLifecyclePolicy(repoName, policyText string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[repoName]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	r.LifecyclePolicy = policyText
	return nil
}

func (s *MemoryECRStore) GetLifecyclePolicy(repoName string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[repoName]
	if !ok {
		return "", &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	if r.LifecyclePolicy == "" {
		return "", &ECRError{Code: "LifecyclePolicyNotFoundException", Message: "Lifecycle policy does not exist", Status: 400}
	}
	return r.LifecyclePolicy, nil
}

func (s *MemoryECRStore) DeleteLifecyclePolicy(repoName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[repoName]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	if r.LifecyclePolicy == "" {
		return &ECRError{Code: "LifecyclePolicyNotFoundException", Message: "Lifecycle policy does not exist", Status: 400}
	}
	r.LifecyclePolicy = ""
	return nil
}

func (s *MemoryECRStore) PutRepositoryPolicy(repoName, policyText string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[repoName]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	r.RepositoryPolicy = policyText
	return nil
}

func (s *MemoryECRStore) GetRepositoryPolicy(repoName string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.repos[repoName]
	if !ok {
		return "", &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	if r.RepositoryPolicy == "" {
		return "", &ECRError{Code: "RepositoryPolicyNotFoundException", Message: "Repository policy does not exist", Status: 400}
	}
	return r.RepositoryPolicy, nil
}

func (s *MemoryECRStore) DeleteRepositoryPolicy(repoName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.repos[repoName]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: fmt.Sprintf("The repository with name '%s' does not exist in the registry", repoName), Status: 400}
	}
	if r.RepositoryPolicy == "" {
		return &ECRError{Code: "RepositoryPolicyNotFoundException", Message: "Repository policy does not exist", Status: 400}
	}
	r.RepositoryPolicy = ""
	return nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (s *MemoryECRStore) AddTags(arn string, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.arnToName[arn]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: "The repository was not found", Status: 400}
	}
	r := s.repos[name]
	if r.Tags == nil {
		r.Tags = make(map[string]string)
	}
	for k, v := range tags {
		r.Tags[k] = v
	}
	return nil
}

func (s *MemoryECRStore) RemoveTags(arn string, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.arnToName[arn]
	if !ok {
		return &ECRError{Code: "RepositoryNotFoundException", Message: "The repository was not found", Status: 400}
	}
	r := s.repos[name]
	for _, k := range keys {
		delete(r.Tags, k)
	}
	return nil
}

func (s *MemoryECRStore) GetTags(arn string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name, ok := s.arnToName[arn]
	if !ok {
		return nil
	}
	out := make(map[string]string, len(s.repos[name].Tags))
	for k, v := range s.repos[name].Tags {
		out[k] = v
	}
	return out
}

// ─── Pull-Through Cache Rules ─────────────────────────────────────────────────

func (s *MemoryECRStore) CreatePullThroughCacheRule(rule *PullThroughCacheRule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ptcRules[rule.EcrRepositoryPrefix]; exists {
		return &ECRError{Code: "PullThroughCacheRuleAlreadyExistsException", Message: fmt.Sprintf("A pull through cache rule for '%s' already exists", rule.EcrRepositoryPrefix), Status: 400}
	}
	s.ptcRules[rule.EcrRepositoryPrefix] = rule
	return nil
}

func (s *MemoryECRStore) ListPullThroughCacheRules() []*PullThroughCacheRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*PullThroughCacheRule
	for _, r := range s.ptcRules {
		cp := *r
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EcrRepositoryPrefix < out[j].EcrRepositoryPrefix })
	return out
}

func (s *MemoryECRStore) DeletePullThroughCacheRule(prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ptcRules[prefix]; !exists {
		return &ECRError{Code: "PullThroughCacheRuleNotFoundException", Message: fmt.Sprintf("Pull through cache rule for '%s' was not found", prefix), Status: 400}
	}
	delete(s.ptcRules, prefix)
	return nil
}

// ─── Registry-Level ───────────────────────────────────────────────────────────

func (s *MemoryECRStore) PutRegistryPolicy(policy string) {
	s.mu.Lock()
	s.registryPolicy = policy
	s.mu.Unlock()
}

func (s *MemoryECRStore) GetRegistryPolicy() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.registryPolicy == "" {
		return "", &ECRError{Code: "RegistryPolicyNotFoundException", Message: "Registry policy does not exist", Status: 400}
	}
	return s.registryPolicy, nil
}

func (s *MemoryECRStore) DeleteRegistryPolicy() {
	s.mu.Lock()
	s.registryPolicy = ""
	s.mu.Unlock()
}

func (s *MemoryECRStore) PutReplicationConfiguration(cfg string) {
	s.mu.Lock()
	s.replicationConfig = cfg
	s.mu.Unlock()
}

func (s *MemoryECRStore) GetReplicationConfiguration() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.replicationConfig
}

// ─── Admin ────────────────────────────────────────────────────────────────────

func (s *MemoryECRStore) Reset() {
	s.mu.Lock()
	s.repos = make(map[string]*Repository)
	s.arnToName = make(map[string]string)
	s.ptcRules = make(map[string]*PullThroughCacheRule)
	s.registryPolicy = ""
	s.replicationConfig = ""
	s.mu.Unlock()
}

type ecrSnapshot struct {
	Repos             map[string]*Repository          `json:"repos"`
	PTCRules          map[string]*PullThroughCacheRule `json:"ptc_rules"`
	RegistryPolicy    string                          `json:"registry_policy"`
	ReplicationConfig string                          `json:"replication_config"`
}

func (s *MemoryECRStore) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := ecrSnapshot{
		Repos:             s.repos,
		PTCRules:          s.ptcRules,
		RegistryPolicy:    s.registryPolicy,
		ReplicationConfig: s.replicationConfig,
	}
	return json.Marshal(snap)
}

func (s *MemoryECRStore) Restore(data json.RawMessage) error {
	var snap ecrSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repos = snap.Repos
	if s.repos == nil {
		s.repos = make(map[string]*Repository)
	}
	// Rebuild arnToName
	s.arnToName = make(map[string]string, len(s.repos))
	for name, r := range s.repos {
		s.arnToName[r.ARN] = name
		if r.Images == nil {
			r.Images = make(map[string]*Image)
		}
	}
	s.ptcRules = snap.PTCRules
	if s.ptcRules == nil {
		s.ptcRules = make(map[string]*PullThroughCacheRule)
	}
	s.registryPolicy = snap.RegistryPolicy
	s.replicationConfig = snap.ReplicationConfig
	return nil
}

// ─── Errors ───────────────────────────────────────────────────────────────────

type ECRError struct {
	Code    string
	Message string
	Status  int
}

func (e *ECRError) Error() string { return e.Code + ": " + e.Message }

// AsECRError returns the ECRError if err is one, nil otherwise.
func AsECRError(err error) *ECRError {
	var e *ECRError
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func cloneRepo(r *Repository) *Repository {
	cp := *r
	cp.Tags = cloneStringMap(r.Tags)
	cp.Images = make(map[string]*Image, len(r.Images))
	for k, v := range r.Images {
		cp.Images[k] = cloneImage(v)
	}
	return &cp
}

func cloneImage(img *Image) *Image {
	cp := *img
	cp.Tags = append([]string(nil), img.Tags...)
	return &cp
}

func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func findImage(r *Repository, id ImageIdentifier) *Image {
	if id.ImageDigest != "" {
		return r.Images[id.ImageDigest]
	}
	if id.ImageTag != "" {
		for _, img := range r.Images {
			for _, t := range img.Tags {
				if t == id.ImageTag {
					return img
				}
			}
		}
	}
	return nil
}

func mergeTags(existing, newTags []string) []string {
	seen := make(map[string]bool, len(existing))
	for _, t := range existing {
		seen[t] = true
	}
	out := append([]string(nil), existing...)
	for _, t := range newTags {
		if !seen[t] {
			out = append(out, t)
		}
	}
	return out
}
