// Package parameter provides SSM Parameter Store storage and business logic.
package parameter

import (
	"context"
	"errors"
	"time"
)

var (
	ErrParameterNotFound    = errors.New("parameter not found")
	ErrAlreadyExists        = errors.New("parameter already exists")
	ErrVersionNotFound      = errors.New("parameter version not found")
)

// ParameterEntry holds metadata and the encrypted value for a parameter.
type ParameterEntry struct {
	AccountID   string
	Region      string
	Name        string
	Type        string // "String" | "StringList" | "SecureString"
	Description string
	KMSKeyID    string // only for SecureString
	Value       []byte // AES-GCM ciphertext for SecureString; plaintext bytes otherwise
	Version     int64
	Tier        string // "Standard" | "Advanced"
	Tags        map[string]string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// HistoryEntry records a previous version of a parameter.
type HistoryEntry struct {
	Name      string
	Version   int64
	Type      string
	KMSKeyID  string
	Value     []byte
	CreatedAt time.Time
}

// ParameterStore is the persistence interface for SSM Parameter Store.
type ParameterStore interface {
	PutParameter(ctx context.Context, e *ParameterEntry, overwrite bool) error
	GetParameter(ctx context.Context, accountID, name string) (ParameterEntry, error)
	DeleteParameter(ctx context.Context, accountID, name string) error
	ListParameters(ctx context.Context, accountID, path string, recursive bool) ([]ParameterEntry, error)
	GetParameterHistory(ctx context.Context, accountID, name string) ([]HistoryEntry, error)

	// Label operations
	LabelParameterVersion(ctx context.Context, accountID, name string, version int64, labels []string) ([]string, error)
	UnlabelParameterVersion(ctx context.Context, accountID, name string, version int64, labels []string) error
	GetLabelsByVersion(ctx context.Context, accountID, name string, version int64) ([]string, error)

	Reset(ctx context.Context)
}
