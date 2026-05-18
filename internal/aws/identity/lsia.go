package identity

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"os"
	"strings"
)

const (
	// LSIAPrefix is the prefix emitted by JaisCloud for session credentials.
	LSIAPrefix = "LSIA"
	// LKIAPrefix is the alternative LocalStack-compatible prefix.
	LKIAPrefix = "LKIA"

	// accountOffset matches LocalStack's ACCOUNT_OFFSET:
	// int.from_bytes(b32decode(b"QAAAAAAA"), "big") = 549755813888
	accountOffset  = uint64(549755813888)
	lsiaTotalLen   = 20
	lsiaPayloadLen = 16 // chars after the 4-char prefix
)

// alphabet is the RFC 4648 standard base32 alphabet.
// Must match LocalStack exactly for cross-emulator parity.
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

// stdBase32NoPad is the standard (non-padded-aware) base32 codec.
// We use StdEncoding; 5 bytes → exactly 8 chars with a trailing "=" that we trim.
var stdBase32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// parityEnabled mirrors LocalStack's PARITY_AWS_ACCESS_KEY_ID flag.
// When true, AKIA/ASIA prefixed keys are also decoded with the LSIA scheme.
func parityEnabled() bool {
	return strings.ToLower(os.Getenv("JAISCLOUD_PARITY_AWS_ACCESS_KEY_ID")) == "true"
}

// EncodeLSIA produces a 20-char LSIA-prefixed access key that embeds a
// 12-digit account ID in a LocalStack-compatible encoding (§9.1.2).
//
// The produced key round-trips through DecodeLSIA and through LocalStack's
// extract_account_id_from_access_key_id — this is a hard parity requirement.
func EncodeLSIA(account string) (string, error) {
	if !TwelveDigit.MatchString(account) {
		return "", fmt.Errorf("invalid account id %q: must be exactly 12 digits", account)
	}

	// Parse account string to uint64.
	var n uint64
	for _, c := range account {
		n = n*10 + uint64(c-'0')
	}
	if n > 999_999_999_999 {
		return "", fmt.Errorf("account %q overflows 12-digit range", account)
	}

	high := n / 2
	parity := byte(n & 1)

	// Pack (high + offset) into 5 bytes, big-endian.
	val := high + accountOffset
	five := [5]byte{
		byte(val >> 32),
		byte(val >> 24),
		byte(val >> 16),
		byte(val >> 8),
		byte(val),
	}

	// Standard base32 over 5 bytes → exactly 8 chars (no padding).
	accountPart := stdBase32.EncodeToString(five[:]) // 8 uppercase chars

	// Pick a deterministic parity char within the appropriate alphabet half.
	// LocalStack picks randomly within the half; we pick index 0 or 16 for
	// reproducibility in tests. The only constraint is index ≥ 16 ↔ parity=1.
	var parityChar byte
	if parity == 1 {
		parityChar = alphabet[16] // 'Q'
	} else {
		parityChar = alphabet[0] // 'A'
	}

	// 7 random chars from the base32 alphabet for entropy/uniqueness.
	rb := make([]byte, 7)
	if _, err := rand.Read(rb); err != nil {
		return "", fmt.Errorf("reading random bytes: %w", err)
	}
	tail := make([]byte, 7)
	for i, b := range rb {
		tail[i] = alphabet[int(b)%32]
	}

	out := LSIAPrefix + accountPart + string(parityChar) + string(tail)
	if len(out) != lsiaTotalLen {
		return "", fmt.Errorf("internal: LSIA length %d, want %d", len(out), lsiaTotalLen)
	}

	// Self-check: round-trip must reproduce the original account.
	got, ok := DecodeLSIA(out)
	if !ok || got != account {
		return "", fmt.Errorf("internal: LSIA self-check failed for %q (decoded %q, ok=%v)", account, got, ok)
	}
	return out, nil
}

// DecodeLSIA extracts the 12-digit account ID from an LSIA-/LKIA-/ASIA-/AKIA-
// prefixed 20-char access key. Returns ("", false) for any malformed input.
// Reference: localstack-core/localstack/aws/accounts.py:21-57.
func DecodeLSIA(accessKey string) (string, bool) {
	if len(accessKey) < lsiaTotalLen {
		return "", false
	}
	prefix := accessKey[:4]
	if prefix != LSIAPrefix && prefix != LKIAPrefix && prefix != "ASIA" && prefix != "AKIA" {
		return "", false
	}
	payload := accessKey[4:]
	if len(payload) < 9 {
		return "", false
	}
	accountPart := payload[:8]
	parityChar := payload[8]

	five, err := stdBase32.DecodeString(strings.ToUpper(accountPart))
	if err != nil || len(five) != 5 {
		return "", false
	}

	val := uint64(five[0])<<32 |
		uint64(five[1])<<24 |
		uint64(five[2])<<16 |
		uint64(five[3])<<8 |
		uint64(five[4])

	if val < accountOffset {
		return "", false
	}
	high := val - accountOffset

	// Determine parity bit from the parity char's position in the alphabet.
	parityIdx := strings.IndexByte(alphabet, parityChar)
	if parityIdx < 0 {
		return "", false
	}
	parityBit := uint64(0)
	if parityIdx >= 16 {
		parityBit = 1
	}

	n := 2*high + parityBit
	if n > 999_999_999_999 {
		return "", false
	}
	return fmt.Sprintf("%012d", n), true
}
