package main

import "testing"

// TestGRPCPortFromEnvPrecedence locks in the canonical-env rule: CHORA_GRPC_PORT
// wins, the legacy GRPC_PORT alias is honoured only when the canonical variable
// is unset, and 9090 is the final default.
func TestGRPCPortFromEnvPrecedence(t *testing.T) {
	cases := []struct {
		name      string
		canonical string
		legacy    string
		want      string
	}{
		{"canonical wins over legacy", "9100", "9200", "9100"},
		{"legacy used when canonical unset", "", "9200", "9200"},
		{"default when both unset", "", "", "9090"},
		{"canonical alone", "9100", "", "9100"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.canonical != "" {
				t.Setenv("CHORA_GRPC_PORT", tc.canonical)
			} else {
				t.Setenv("CHORA_GRPC_PORT", "")
			}
			if tc.legacy != "" {
				t.Setenv("GRPC_PORT", tc.legacy)
			} else {
				t.Setenv("GRPC_PORT", "")
			}
			if got := grpcPortFromEnv(); got != tc.want {
				t.Errorf("grpcPortFromEnv() = %q; want %q", got, tc.want)
			}
			if got, want := grpcEnvIsCanonical(), tc.canonical != ""; got != want {
				t.Errorf("grpcEnvIsCanonical() = %v; want %v", got, want)
			}
		})
	}
}
