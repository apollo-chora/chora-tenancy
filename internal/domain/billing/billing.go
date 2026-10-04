// Package billing is the pure-domain core of the Billing sub-domain.
package billing

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrInvalidPeriod   = errors.New("invalid period")
)

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

type SubscriptionUsage struct {
	AddOnID           string
	MonthlyPriceCents int64
}

type UsageLine struct {
	AddOnID        string
	UnitPriceCents int64
	Quantity       int
	LineTotalCents int64
}

type UsageSummary struct {
	TenantID    string
	PeriodStart time.Time
	PeriodEnd   time.Time
	Lines       []UsageLine
	TotalCents  int64
}

func AggregateUsage(tenantID string, periodStart, periodEnd time.Time, subs []SubscriptionUsage) (*UsageSummary, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if !periodEnd.After(periodStart) {
		return nil, fmt.Errorf("%w: period_end must be after period_start", ErrInvalidPeriod)
	}
	lines := make([]UsageLine, 0, len(subs))
	var total int64
	for _, s := range subs {
		lines = append(lines, UsageLine{
			AddOnID:        s.AddOnID,
			UnitPriceCents: s.MonthlyPriceCents,
			Quantity:       1,
			LineTotalCents: s.MonthlyPriceCents,
		})
		total += s.MonthlyPriceCents
	}
	return &UsageSummary{
		TenantID:    tenantID,
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Lines:       lines,
		TotalCents:  total,
	}, nil
}

type InvoiceStatus string

const (
	InvoiceStatusIssued   InvoiceStatus = "issued"
	InvoiceStatusPaid     InvoiceStatus = "paid"
	InvoiceStatusFailed   InvoiceStatus = "failed"
	InvoiceStatusRefunded InvoiceStatus = "refunded"
)

type Invoice struct {
	ID              string
	TenantID        string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	Lines           []UsageLine
	TotalCents      int64
	Currency        string
	Status          InvoiceStatus
	StripeInvoiceID string
	IssuedAt        time.Time
	UpdatedAt       time.Time
}

func NewInvoiceFromSummary(s *UsageSummary) (*Invoice, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: summary required", ErrInvalidArgument)
	}
	if strings.TrimSpace(s.TenantID) == "" {
		return nil, fmt.Errorf("%w: summary.tenant_id required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &Invoice{
		ID:          NewUUIDv7(),
		TenantID:    s.TenantID,
		PeriodStart: s.PeriodStart,
		PeriodEnd:   s.PeriodEnd,
		Lines:       append([]UsageLine(nil), s.Lines...),
		TotalCents:  s.TotalCents,
		Currency:    "SGD",
		Status:      InvoiceStatusIssued,
		IssuedAt:    now,
		UpdatedAt:   now,
	}, nil
}

func (i *Invoice) AttachStripeID(stripeID string) {
	i.StripeInvoiceID = stripeID
	i.UpdatedAt = time.Now().UTC()
}

func (i *Invoice) MarkPaid()     { i.Status = InvoiceStatusPaid; i.UpdatedAt = time.Now().UTC() }
func (i *Invoice) MarkFailed()   { i.Status = InvoiceStatusFailed; i.UpdatedAt = time.Now().UTC() }
func (i *Invoice) MarkRefunded() { i.Status = InvoiceStatusRefunded; i.UpdatedAt = time.Now().UTC() }

type InvoiceRepo struct {
	mu       sync.RWMutex
	byID     map[string]*Invoice
	byTenant map[string][]*Invoice
}

func NewInvoiceRepo() *InvoiceRepo {
	return &InvoiceRepo{
		byID:     make(map[string]*Invoice),
		byTenant: make(map[string][]*Invoice),
	}
}

func (r *InvoiceRepo) Save(inv *Invoice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[inv.ID]; exists {
		return
	}
	r.byID[inv.ID] = inv
	r.byTenant[inv.TenantID] = append(r.byTenant[inv.TenantID], inv)
}

func (r *InvoiceRepo) Get(id string) (*Invoice, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	inv, ok := r.byID[id]
	return inv, ok
}

func (r *InvoiceRepo) ListByTenant(tenantID string) []*Invoice {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := append([]*Invoice(nil), r.byTenant[tenantID]...)
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}

type PaymentMethodStatus string

const (
	PaymentMethodStatusActive   PaymentMethodStatus = "active"
	PaymentMethodStatusDetached PaymentMethodStatus = "detached"
)

type PaymentMethod struct {
	ID          string
	TenantID    string
	StripeToken string
	Status      PaymentMethodStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func NewPaymentMethod(tenantID, stripeToken string) (*PaymentMethod, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: tenant_id required", ErrInvalidArgument)
	}
	if strings.TrimSpace(stripeToken) == "" {
		return nil, fmt.Errorf("%w: stripe_token required", ErrInvalidArgument)
	}
	now := time.Now().UTC()
	return &PaymentMethod{
		ID:          NewUUIDv7(),
		TenantID:    tenantID,
		StripeToken: stripeToken,
		Status:      PaymentMethodStatusActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (p *PaymentMethod) Detach() {
	if p.Status == PaymentMethodStatusDetached {
		return
	}
	p.Status = PaymentMethodStatusDetached
	p.UpdatedAt = time.Now().UTC()
}

type PaymentMethodRepo struct {
	mu       sync.RWMutex
	byID     map[string]*PaymentMethod
	byTenant map[string][]*PaymentMethod
}

func NewPaymentMethodRepo() *PaymentMethodRepo {
	return &PaymentMethodRepo{
		byID:     make(map[string]*PaymentMethod),
		byTenant: make(map[string][]*PaymentMethod),
	}
}

func (r *PaymentMethodRepo) Save(pm *PaymentMethod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[pm.ID]; !exists {
		r.byTenant[pm.TenantID] = append(r.byTenant[pm.TenantID], pm)
	}
	r.byID[pm.ID] = pm
}

func (r *PaymentMethodRepo) ListByTenant(tenantID string) []*PaymentMethod {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := append([]*PaymentMethod(nil), r.byTenant[tenantID]...)
	sort.Slice(out, func(i, j int) bool { return strings.Compare(out[i].ID, out[j].ID) < 0 })
	return out
}
