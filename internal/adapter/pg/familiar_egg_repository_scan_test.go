package pg_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/adapter/pg"
)

// Exercise the NullTime-Valid branches in scanPurchase + scanCatalog.

func TestPurchaseRepository_GetByStripeSessionID_AllNullTimesSet(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQueryRunner{
		rowResponses: []func(dest ...any) error{
			fullPurchaseScanFn(now),
		},
	}
	r := pg.NewPurchaseRepository(q)
	p, ok, err := r.GetByStripeSessionID(context.Background(), "cs_full")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("not found")
	}
	if p.PaidAt == nil || p.FailedAt == nil || p.RefundedAt == nil || p.ExpiredAt == nil || p.ProvisionedAt == nil {
		t.Errorf("expected all NullTimes populated; got %+v", p)
	}
}

func TestCatalogRepository_GetBySKU_WithAvailabilityAndDeleted(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	q := &stubQueryRunner{
		rowResponses: []func(dest ...any) error{
			fullCatalogScanFn(now),
		},
	}
	r := pg.NewCatalogRepository(q)
	e, ok, err := r.GetBySKU(context.Background(), catalogTestTenantID, "egg.full.v1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("not found")
	}
	if e.AvailableFrom == nil || e.AvailableUntil == nil || e.DeletedAt == nil {
		t.Errorf("expected all NullTimes populated; got %+v", e)
	}
}

func fullPurchaseScanFn(now time.Time) func(dest ...any) error {
	soft := now.AddDate(0, 0, 30)
	hard := now.AddDate(0, 0, 60)
	return func(dest ...any) error {
		mustString(dest[0], "01970000-0000-7000-8000-000000000001")
		mustString(dest[1], "01970000-0000-7000-8000-000000000010")
		mustString(dest[2], "01970000-0000-7000-8000-000000000020")
		mustString(dest[3], "egg.standard.v1")
		mustString(dest[4], "")
		mustString(dest[5], "expired")
		mustInt64(dest[6], 999)
		mustInt64(dest[7], 999)
		mustInt64(dest[8], 999)
		mustString(dest[9], "SGD")
		mustString(dest[10], "cs_full")
		mustString(dest[11], "pi_x")
		mustString(dest[12], "ch_x")
		mustString(dest[13], "re_x")
		mustString(dest[14], "https://x")
		mustString(dest[15], "card_declined")
		mustString(dest[16], "msg")
		mustString(dest[17], "hard_expiry_unhatched")
		mustBool(dest[18], true)
		mustTime(dest[19], now) // checkout_started_at
		mustNullTimeValid(dest[20], now.Add(time.Minute))
		mustNullTimeValid(dest[21], now.Add(time.Hour))
		mustNullTimeValid(dest[22], now.Add(2*time.Hour))
		mustNullTimeValid(dest[23], now.Add(3*time.Hour))
		mustNullTimeValid(dest[24], now.Add(4*time.Hour))
		mustTime(dest[25], soft)
		mustTime(dest[26], hard)
		mustTime(dest[27], now)
		mustTime(dest[28], now)
		return nil
	}
}

func fullCatalogScanFn(now time.Time) func(dest ...any) error {
	return func(dest ...any) error {
		mustString(dest[0], "egg.full.v1")
		mustString(dest[1], "")
		mustString(dest[2], "Full")
		mustString(dest[3], "desc")
		mustInt64(dest[4], 1500)
		mustString(dest[5], "SGD")
		mustString(dest[6], "01970000-0000-7000-8000-000000000099")
		mustString(dest[7], `{"owl":50,"fox":50}`)
		mustTime(dest[8], now)
		mustBool(dest[9], true)
		mustBool(dest[10], false)
		mustInt(dest[11], 30)
		mustInt(dest[12], 60)
		mustNullTimeValid(dest[13], now.Add(-24*time.Hour))
		mustNullTimeValid(dest[14], now.Add(24*time.Hour))
		mustTime(dest[15], now)
		mustTime(dest[16], now)
		mustNullTimeValid(dest[17], now)
		return nil
	}
}

func mustNullTimeValid(dst any, t time.Time) {
	if p, ok := dst.(*sql.NullTime); ok {
		*p = sql.NullTime{Time: t, Valid: true}
	}
}
