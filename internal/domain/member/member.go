// Package member is the pure-domain core of the Member management sub-domain.
package member

import (
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrBatchTooLarge   = errors.New("batch too large (max 1000)")
	ErrUnknownRole     = errors.New("unknown role")
)

const MaxBatchSize = 1000

var validRoles = map[string]struct{}{
	"super_admin":     {},
	"platform_ops":    {},
	"tenant_admin":    {},
	"billing_admin":   {},
	"instructor":      {},
	"trainer":         {},
	"supervisor":      {},
	"content_manager": {},
	"support_agent":   {},
	"learner":         {},
	"guardian":        {},
}

func IsValidRole(role string) bool {
	_, ok := validRoles[role]
	return ok
}

func NewUUIDv7() string {
	const buflen = 16
	var b [buflen]byte
	now := uint64(time.Now().UnixMilli())
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	if _, err := rand.Read(b[6:]); err != nil {
		for i := 6; i < buflen; i++ {
			b[i] = byte(now >> uint(8*(i-6)))
		}
	}
	b[6] = (b[6] & 0x0F) | 0x70
	b[8] = (b[8] & 0x3F) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type Status string

const (
	StatusInvited   Status = "invited"
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
)

type Member struct {
	ID               string
	TenantID         string
	Email            string
	Roles            []string
	GCID             string
	Status           Status
	SuspensionReason string
	InvitedAt        time.Time
	ActivatedAt      *time.Time
	UpdatedAt        time.Time
}

var emailRE = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

func normaliseEmail(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func validateRoles(roles []string) error {
	if len(roles) == 0 {
		return fmt.Errorf("%w: at least one role required", ErrInvalidArgument)
	}
	for _, r := range roles {
		if !IsValidRole(r) {
			return fmt.Errorf("%w: %s", ErrUnknownRole, r)
		}
	}
	return nil
}

func NewMember(tenantID, email string, roles []string) (*Member, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	em := normaliseEmail(email)
	if em == "" {
		return nil, fmt.Errorf("%w: email required", ErrInvalidArgument)
	}
	if !emailRE.MatchString(em) {
		return nil, fmt.Errorf("%w: invalid email %q", ErrInvalidArgument, em)
	}
	if err := validateRoles(roles); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	rolesCopy := append([]string(nil), roles...)
	return &Member{
		ID:        NewUUIDv7(),
		TenantID:  tenantID,
		Email:     em,
		Roles:     rolesCopy,
		Status:    StatusInvited,
		InvitedAt: now,
		UpdatedAt: now,
	}, nil
}

func (m *Member) Activate(gcid string) {
	if m.Status == StatusActive {
		return
	}
	m.Status = StatusActive
	m.GCID = gcid
	now := time.Now().UTC()
	m.ActivatedAt = &now
	m.UpdatedAt = now
}

func (m *Member) Suspend(reason string) {
	m.Status = StatusSuspended
	m.SuspensionReason = reason
	m.UpdatedAt = time.Now().UTC()
}

func (m *Member) SuspendWithError(reason string) error {
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("%w: suspension reason required", ErrInvalidArgument)
	}
	m.Suspend(reason)
	return nil
}

func (m *Member) UpdateRoles(roles []string) error {
	if err := validateRoles(roles); err != nil {
		return err
	}
	m.Roles = append([]string(nil), roles...)
	m.UpdatedAt = time.Now().UTC()
	return nil
}

type InviteRequest struct {
	Email string
	Roles []string
}

type BulkInviteResult struct {
	Invited        []*Member
	DuplicateCount int
	RejectedCount  int
	AlreadyExists  int
}

func BulkInvite(tenantID string, reqs []InviteRequest) (*BulkInviteResult, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if len(reqs) > MaxBatchSize {
		return nil, ErrBatchTooLarge
	}
	out := &BulkInviteResult{}
	seen := map[string]struct{}{}
	for _, req := range reqs {
		em := normaliseEmail(req.Email)
		if em == "" {
			out.RejectedCount++
			continue
		}
		if !emailRE.MatchString(em) {
			out.RejectedCount++
			continue
		}
		if len(req.Roles) == 0 {
			out.RejectedCount++
			continue
		}
		validRow := true
		for _, r := range req.Roles {
			if !IsValidRole(r) {
				validRow = false
				break
			}
		}
		if !validRow {
			out.RejectedCount++
			continue
		}
		if _, dup := seen[em]; dup {
			out.DuplicateCount++
			continue
		}
		seen[em] = struct{}{}
		m, err := NewMember(tenantID, em, req.Roles)
		if err != nil {
			out.RejectedCount++
			continue
		}
		out.Invited = append(out.Invited, m)
	}
	return out, nil
}

type ListFilter struct {
	Role   string
	Status string
}

type Registry struct {
	mu       sync.RWMutex
	byID     map[string]*Member
	byTenant map[string][]*Member
	byEmail  map[string]*Member
}

func NewRegistry() *Registry {
	return &Registry{
		byID:     make(map[string]*Member),
		byTenant: make(map[string][]*Member),
		byEmail:  make(map[string]*Member),
	}
}

func (r *Registry) Save(m *Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[m.ID]; !exists {
		r.byTenant[m.TenantID] = append(r.byTenant[m.TenantID], m)
	}
	r.byID[m.ID] = m
	r.byEmail[m.TenantID+"|"+m.Email] = m
}

func (r *Registry) Get(id string) (*Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.byID[id]
	return m, ok
}

func (r *Registry) FindByEmail(tenantID, email string) (*Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.byEmail[tenantID+"|"+normaliseEmail(email)]
	return m, ok
}

func (r *Registry) ListByTenant(tenantID string, f ListFilter) []*Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	all := r.byTenant[tenantID]
	out := make([]*Member, 0, len(all))
	for _, m := range all {
		if f.Role != "" {
			has := false
			for _, role := range m.Roles {
				if role == f.Role {
					has = true
					break
				}
			}
			if !has {
				continue
			}
		}
		if f.Status != "" && string(m.Status) != f.Status {
			continue
		}
		out = append(out, m)
	}
	return out
}

func (r *Registry) BulkInvite(tenantID string, reqs []InviteRequest) (*BulkInviteResult, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if len(reqs) > MaxBatchSize {
		return nil, ErrBatchTooLarge
	}
	out := &BulkInviteResult{}
	seen := map[string]struct{}{}
	for _, req := range reqs {
		em := normaliseEmail(req.Email)
		if em == "" {
			out.RejectedCount++
			continue
		}
		if !emailRE.MatchString(em) {
			out.RejectedCount++
			continue
		}
		if len(req.Roles) == 0 {
			out.RejectedCount++
			continue
		}
		valid := true
		for _, r := range req.Roles {
			if !IsValidRole(r) {
				valid = false
				break
			}
		}
		if !valid {
			out.RejectedCount++
			continue
		}
		if _, dup := seen[em]; dup {
			out.DuplicateCount++
			continue
		}
		seen[em] = struct{}{}
		if _, exists := r.FindByEmail(tenantID, em); exists {
			out.AlreadyExists++
			continue
		}
		m, err := NewMember(tenantID, em, req.Roles)
		if err != nil {
			out.RejectedCount++
			continue
		}
		r.Save(m)
		out.Invited = append(out.Invited, m)
	}
	return out, nil
}
