// Package ses implements the SES v1 provider (email service).
package ses

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtSESIdentity = "ses_identity"
	maxSentRing   = 1000
)

type Provider struct {
	resources  store.ResourceStore
	mu         sync.Mutex
	sentEmails []sentEmail
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"SES.SendEmail":                        p.SendEmail,
		"SES.SendRawEmail":                     p.SendRawEmail,
		"SES.SendBulkTemplatedEmail":           p.SendBulkTemplatedEmail,
		"SES.VerifyEmailIdentity":              p.VerifyEmailIdentity,
		"SES.VerifyDomainIdentity":             p.VerifyDomainIdentity,
		"SES.ListIdentities":                   p.ListIdentities,
		"SES.DeleteIdentity":                   p.DeleteIdentity,
		"SES.GetIdentityVerificationAttributes": p.GetIdentityVerificationAttributes,
		"SES.GetSendQuota":                     p.GetSendQuota,
		"SES.GetSendStatistics":                p.GetSendStatistics,
	}
}

// Reset clears sent emails (implements admin.Resetter indirectly via store).
func (p *Provider) Reset() {
	p.mu.Lock()
	p.sentEmails = nil
	p.mu.Unlock()
}

// SentEmails returns a copy of the ring buffer (for admin inspection).
func (p *Provider) SentEmails() []sentEmail {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]sentEmail, len(p.sentEmails))
	copy(out, p.sentEmails)
	return out
}

type sesIdentity struct {
	Identity           string `json:"Identity"`
	IdentityType       string `json:"IdentityType"` // "EmailAddress" or "Domain"
	VerificationStatus string `json:"VerificationStatus"`
}

type sentEmail struct {
	MessageID   string    `json:"MessageId"`
	From        string    `json:"From"`
	Destination []string  `json:"Destination"`
	Subject     string    `json:"Subject"`
	Body        string    `json:"Body"`
	SentAt      time.Time `json:"SentAt"`
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newMessageID() string {
	return fmt.Sprintf("%s-%s-%s-%s@email.amazonses.com", randHex(4), randHex(4), randHex(4), randHex(8))
}

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func sesErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func (p *Provider) addSent(email sentEmail) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sentEmails = append(p.sentEmails, email)
	if len(p.sentEmails) > maxSentRing {
		p.sentEmails = p.sentEmails[len(p.sentEmails)-maxSentRing:]
	}
}

func (p *Provider) verifyIdentity(ctx context.Context, identity, idType string) {
	id := sesIdentity{
		Identity:           identity,
		IdentityType:       idType,
		VerificationStatus: "Success",
	}
	data, _ := json.Marshal(id)
	entry := store.ResourceEntry{Type: rtSESIdentity, ID: identity, Data: data}
	if err := p.resources.Create(ctx, entry); err == store.ErrAlreadyExists {
		p.resources.Update(ctx, entry)
	}
}

// isVerified checks whether the given email address (or its domain) is
// verified in the store. Returns true if verified, false otherwise.
func (p *Provider) isVerified(ctx context.Context, email string) bool {
	// Check the exact email address.
	if e, err := p.resources.Get(ctx, rtSESIdentity, email); err == nil {
		var id sesIdentity
		if json.Unmarshal(e.Data, &id) == nil && id.VerificationStatus == "Success" {
			return true
		}
	}
	// Check the domain part.
	if atIdx := strings.LastIndex(email, "@"); atIdx >= 0 {
		domain := email[atIdx+1:]
		if e, err := p.resources.Get(ctx, rtSESIdentity, domain); err == nil {
			var id sesIdentity
			if json.Unmarshal(e.Data, &id) == nil && id.VerificationStatus == "Success" {
				return true
			}
		}
	}
	return false
}

func (p *Provider) SendEmail(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	source := str(nr.Params, "Source")

	// Enforce sender verification.
	if source != "" && !p.isVerified(ctx, source) {
		return nil, sesErr("MessageRejected",
			fmt.Sprintf("Email address not verified. The following identities failed the check in region US-EAST-1: %s", source),
			http.StatusBadRequest)
	}

	messageID := newMessageID()
	var destinations []string
	if dest, ok := nr.Params["Destination"].(map[string]any); ok {
		if toAddrs, ok := dest["ToAddresses"].([]any); ok {
			for _, a := range toAddrs {
				if s, ok := a.(string); ok {
					destinations = append(destinations, s)
				}
			}
		}
	}
	subject := ""
	body := ""
	if msg, ok := nr.Params["Message"].(map[string]any); ok {
		if sub, ok := msg["Subject"].(map[string]any); ok {
			subject, _ = sub["Data"].(string)
		}
		if bdy, ok := msg["Body"].(map[string]any); ok {
			if txt, ok := bdy["Text"].(map[string]any); ok {
				body, _ = txt["Data"].(string)
			} else if html, ok := bdy["Html"].(map[string]any); ok {
				body, _ = html["Data"].(string)
			}
		}
	}
	p.addSent(sentEmail{MessageID: messageID, From: source, Destination: destinations, Subject: subject, Body: body, SentAt: time.Now().UTC()})
	return provider.OK(map[string]any{"MessageId": messageID}), nil
}

func (p *Provider) SendRawEmail(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	messageID := newMessageID()
	source := str(nr.Params, "Source")
	p.addSent(sentEmail{MessageID: messageID, From: source, SentAt: time.Now().UTC()})
	return provider.OK(map[string]any{"MessageId": messageID}), nil
}

func (p *Provider) SendBulkTemplatedEmail(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	messageID := newMessageID()
	p.addSent(sentEmail{MessageID: messageID, SentAt: time.Now().UTC()})
	return provider.OK(map[string]any{"Status": []map[string]any{{"MessageId": messageID, "Status": "Success"}}}), nil
}

func (p *Provider) VerifyEmailIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	email := str(nr.Params, "EmailAddress")
	if email == "" {
		return nil, sesErr("InvalidParameterValue", "EmailAddress is required", http.StatusBadRequest)
	}
	p.verifyIdentity(ctx, email, "EmailAddress")
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) VerifyDomainIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	domain := str(nr.Params, "Domain")
	if domain == "" {
		return nil, sesErr("InvalidParameterValue", "Domain is required", http.StatusBadRequest)
	}
	p.verifyIdentity(ctx, domain, "Domain")
	token := randHex(32)
	return provider.OK(map[string]any{"VerificationToken": token}), nil
}

func (p *Provider) ListIdentities(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtSESIdentity, "")
	identities := make([]string, 0, len(entries))
	for _, e := range entries {
		var id sesIdentity
		if json.Unmarshal(e.Data, &id) == nil {
			identities = append(identities, id.Identity)
		}
	}
	return provider.OK(map[string]any{"Identities": identities}), nil
}

func (p *Provider) DeleteIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	identity := str(nr.Params, "Identity")
	_ = p.resources.Delete(ctx, rtSESIdentity, identity)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) GetIdentityVerificationAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Identities.member.N
	var identities []string
	for i := 1; ; i++ {
		v := str(nr.Params, fmt.Sprintf("Identities.member.%d", i))
		if v == "" {
			break
		}
		identities = append(identities, v)
	}
	attrs := map[string]any{}
	for _, identity := range identities {
		status := "NotStarted"
		if e, err := p.resources.Get(ctx, rtSESIdentity, identity); err == nil {
			var id sesIdentity
			if json.Unmarshal(e.Data, &id) == nil {
				status = id.VerificationStatus
			}
		}
		entry := map[string]any{"VerificationStatus": status}
		if strings.Contains(identity, ".") && !strings.Contains(identity, "@") {
			entry["VerificationToken"] = randHex(32)
		}
		attrs[identity] = entry
	}
	return provider.OK(map[string]any{"VerificationAttributes": attrs}), nil
}

func (p *Provider) GetSendQuota(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"Max24HourSend":   50000.0,
		"MaxSendRate":     14.0,
		"SentLast24Hours": 0.0,
	}), nil
}

func (p *Provider) GetSendStatistics(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"SendDataPoints": []map[string]any{},
	}), nil
}
