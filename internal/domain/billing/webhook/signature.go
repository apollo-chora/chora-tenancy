package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// VerifySignature validates a Stripe webhook signature against the supplied
// secret + payload + clock. Returns nil on success, a descriptive error
// otherwise.
//
// Stripe-Signature header format:
//
//	t=<unix-seconds>,v1=<hex-hmac-sha256>
//
// Multiple v1 segments are accepted (Stripe rotates secrets by including the
// signature under each active key) — verification passes when ANY v1 matches
// the configured secret.
//
// `now` is injected for tests; production callers pass time.Now().UTC().
// `tolerance` caps the allowed clock skew between the signature timestamp
// and now.
var (
	// ErrEmptySignature signals a missing Stripe-Signature header.
	ErrEmptySignature = errors.New("webhook: empty stripe signature header")
	// ErrSignatureMalformed signals a header that cannot be parsed.
	ErrSignatureMalformed = errors.New("webhook: malformed stripe signature header")
	// ErrSignatureExpired signals a timestamp that's outside tolerance.
	ErrSignatureExpired = errors.New("webhook: stripe signature expired (outside tolerance)")
	// ErrSignatureMismatch signals no v1 candidate matched the secret-HMAC.
	ErrSignatureMismatch = errors.New("webhook: stripe signature mismatch")
	// ErrEmptySecret signals an empty webhook secret.
	ErrEmptySecret = errors.New("webhook: empty webhook secret")
)

// VerifySignature implements the Stripe webhook signature verification
// algorithm. Pure function modulo the system clock and the secret.
func VerifySignature(secret, header string, payload []byte, now time.Time, tolerance time.Duration) error {
	if secret == "" {
		return ErrEmptySecret
	}
	if header == "" {
		return ErrEmptySignature
	}

	tsStr, sigs, err := parseSigHeader(header)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSignatureMalformed, err)
	}

	tsInt, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: timestamp %q not a unix-seconds integer", ErrSignatureMalformed, tsStr)
	}
	signedAt := time.Unix(tsInt, 0).UTC()
	delta := now.Sub(signedAt)
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		return fmt.Errorf("%w: skew %s > tolerance %s", ErrSignatureExpired, delta, tolerance)
	}

	if len(sigs) == 0 {
		return fmt.Errorf("%w: no v1 signature segments", ErrSignatureMalformed)
	}

	signedPayload := fmt.Sprintf("%d.%s", tsInt, payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signedPayload))
	expected := mac.Sum(nil)

	for _, s := range sigs {
		got, err := hex.DecodeString(s)
		if err != nil {
			// Skip malformed-hex candidates — Stripe's docs allow multiple
			// v1 sigs; an invalid one alongside a valid one is still a
			// pass.
			continue
		}
		if hmac.Equal(expected, got) {
			return nil
		}
	}
	return ErrSignatureMismatch
}

// parseSigHeader returns (timestampStr, []sigCandidates, err). Empty timestamp
// or empty sig segments yield ErrSignatureMalformed.
func parseSigHeader(header string) (string, []string, error) {
	parts := strings.Split(header, ",")
	var ts string
	var sigs []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq <= 0 || eq == len(p)-1 {
			return "", nil, fmt.Errorf("segment %q malformed", p)
		}
		key := p[:eq]
		val := p[eq+1:]
		switch key {
		case "t":
			if ts != "" {
				return "", nil, fmt.Errorf("multiple t= segments")
			}
			if val == "" {
				return "", nil, fmt.Errorf("empty t= value")
			}
			ts = val
		case "v1":
			if val == "" {
				return "", nil, fmt.Errorf("empty v1= value")
			}
			sigs = append(sigs, val)
		}
	}
	if ts == "" {
		return "", nil, fmt.Errorf("missing t= segment")
	}
	if len(sigs) == 0 {
		return "", nil, fmt.Errorf("missing v1= segment")
	}
	return ts, sigs, nil
}
