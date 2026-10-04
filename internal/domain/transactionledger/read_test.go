package transactionledger_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-tenancy/internal/domain/transactionledger"
)

func TestParseSort(t *testing.T) {
	cases := []struct {
		in       string
		wantFld  transactionledger.SortField
		wantDesc bool
	}{
		{"", transactionledger.SortByOccurredAt, true},
		{"occurred_at:desc", transactionledger.SortByOccurredAt, true},
		{"OCCURRED_AT:DESC", transactionledger.SortByOccurredAt, true},
		{"occurred_at:asc", transactionledger.SortByOccurredAt, false},
		{"amount:desc", transactionledger.SortByAmount, true},
		{"amount:asc", transactionledger.SortByAmount, false},
		{"garbage", transactionledger.SortByOccurredAt, true},
		{"  amount:asc  ", transactionledger.SortByAmount, false},
	}
	for _, c := range cases {
		got := transactionledger.ParseSort(c.in)
		if got.Field != c.wantFld || got.Desc != c.wantDesc {
			t.Errorf("ParseSort(%q) = {%v,%v}; want {%v,%v}",
				c.in, got.Field, got.Desc, c.wantFld, c.wantDesc)
		}
	}
}

func TestNormalizePageSize(t *testing.T) {
	cases := []struct {
		req, def, want int
	}{
		{0, 20, 20},   // unset → default
		{10, 20, 10},  // allowed
		{20, 20, 20},  // allowed
		{50, 50, 50},  // allowed
		{100, 20, 100},// allowed
		{7, 20, 20},   // out-of-set → default
		{1000, 50, 50},// out-of-set → default
		{-5, 20, 20},  // negative → default
	}
	for _, c := range cases {
		if got := transactionledger.NormalizePageSize(c.req, c.def); got != c.want {
			t.Errorf("NormalizePageSize(%d,%d) = %d; want %d", c.req, c.def, got, c.want)
		}
	}
}
