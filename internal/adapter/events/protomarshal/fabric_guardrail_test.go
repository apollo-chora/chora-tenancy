// fabric_guardrail_test — the Flag-1 CI guardrail from the 2026-07-01
// event-fabric audit. It is the durable regression gate that WOULD HAVE CAUGHT
// the kg_hexagon_fog class of bug: a topic that chora-tenancy PRODUCES and
// that is bound to a BINARY Pub/Sub Schema Registry schema, but for which
// protomarshal has NO encoder case — so the outbox silently JSON-falls-back and
// the binary schema rejects every publish → dead-letter forever.
//
// Data sources (deployed-reality precedence — CLAUDE.md source hierarchy #1):
//
//   - "bound to a binary schema" = a tenancy topic in the m10-data-plane
//     terraform `local.pubsub_topics` map MINUS `local.schemaless_topics`. That
//     terraform IS what provisions the Schema Registry bindings that make binary
//     encoding mandatory, so it is the authoritative source — NOT
//     chora-infra/topics/topics.yaml, which is a stale partial catalogue that
//     (a) lists every tenancy topic with a `schema:` field including ~35
//     declared-only scaffolding topics that legitimately JSON-fall-back, and
//     (b) does not even contain the assessment / submission / grading-lifecycle
//     topics that the audit found broken. topics.yaml is still cross-checked
//     below (TestTopicsYaml_CataloguesFabricRepairTopics) for the two Class-D
//     topics this repair added.
//
//   - "chora-tenancy produces it" = the topic string literal appears in the
//     service's Go source OUTSIDE the encoder (protomarshal), the decoder
//     (protodecode) and the subscribers (consume-only surfaces) and the tests.
//     This scopes the assertion to emitted topics so it does not false-fail on
//     the declared-only scaffolding (booking.created / exam.* / rostering.* /
//     …) that has a schema but no producer + no encoder yet.
//
// NOTE (per the audit): this is a CI TEST only. encodeOutboxPayload MUST NOT be
// made runtime-fail-loud — 57 topics platform-wide legitimately publish
// schemaless JSON, and a runtime blanket fail-loud would break them.
package protomarshal_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-tenancy/internal/adapter/events/protomarshal"
)

// tenancyTopicRe matches a fully-qualified tenancy topic that is a STANDALONE
// double-quoted string literal (Go source string constant, or a TF map key /
// list element). Requiring the surrounding quotes is what distinguishes a real
// producer literal from a topic merely mentioned in a // comment or embedded in
// a larger log-format string (e.g. a subscriber logging the topic it consumes —
// chora.tenancy.grading.oe_batch_completed.v1 is subscribed to, not produced).
var tenancyTopicRe = regexp.MustCompile(`"(chora\.tenancy\.[a-z0-9_.]+\.v1)"`)

// topicLiterals returns the set of tenancy topics quoted as standalone string
// literals in s (capture group 1 of each match).
func topicLiterals(s string) map[string]bool {
	out := map[string]bool{}
	for _, m := range tenancyTopicRe.FindAllStringSubmatch(s, -1) {
		out[m[1]] = true
	}
	return out
}

// repoRoot walks up from this test's source file (stable at build time,
// independent of the test's working directory) to the monorepo root — the
// directory holding both chora-infra/ and services/chora-tenancy/.
// ok is false in the split-repo layout, where this repository is standalone and
// the monorepo root does not exist above it.
func repoRoot(t *testing.T) (string, bool) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed — cannot locate repo root")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 15; i++ {
		infra := filepath.Join(dir, "chora-infra", "terraform", "modules", "m10-data-plane", "main.tf")
		svc := filepath.Join(dir, "services", "chora-tenancy")
		if fileExists(infra) && dirExists(svc) {
			return dir, true
		}
		// Stop AT the monorepo root (the directory holding go.work). Without
		// this the walk climbs past a git worktree into whatever checkout
		// contains it, reads THAT tree's copy and reports green on a file this
		// tree does not have: exactly how the missed half of the ADR-254 D9
		// rename hid, because the parent checkout still had the old path.
		if _, gerr := os.Stat(filepath.Join(dir, "go.work")); gerr == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", false
}

func fileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// tenancyBinaryBoundTopics returns the set of tenancy topics bound to a
// binary Schema Registry schema = every tenancy topic literal in the
// m10-data-plane terraform MINUS the ones in local.schemaless_topics.
func tenancyBinaryBoundTopics(t *testing.T, root string) map[string]bool {
	t.Helper()
	tfPath := filepath.Join(root, "chora-infra", "terraform", "modules", "m10-data-plane", "main.tf")
	raw, err := os.ReadFile(tfPath)
	if err != nil {
		t.Fatalf("read m10-data-plane main.tf: %v", err)
	}
	tf := string(raw)

	all := topicLiterals(tf)

	// Extract the schemaless_topics = toset([ ... ]) block and remove its
	// tenancy members — those intentionally publish schemaless JSON.
	schemaless := map[string]bool{}
	if start := strings.Index(tf, "schemaless_topics = toset(["); start >= 0 {
		rest := tf[start:]
		if end := strings.Index(rest, "])"); end >= 0 {
			schemaless = topicLiterals(rest[:end])
		} else {
			t.Fatal("schemaless_topics block has no closing '])' — TF parse assumption broken")
		}
	} else {
		t.Fatal("could not locate local.schemaless_topics in main.tf — TF parse assumption broken")
	}

	bound := map[string]bool{}
	for topic := range all {
		if !schemaless[topic] {
			bound[topic] = true
		}
	}
	if len(bound) == 0 {
		t.Fatal("0 binary-bound tenancy topics parsed from TF — parse is broken (would make the guardrail vacuously green)")
	}
	return bound
}

// preMarshalsItsPayload reports whether a file builds its outbox row from bytes
// it marshalled itself, instead of handing a payload MAP to the publisher.
//
// chora-tenancy has TWO producer paths and only one of them reaches the encoder
// this guardrail tests:
//
//  1. outbox.Publisher.Publish(topic, env, payloadMap) hands a MAP to
//     protomarshal.MarshalPayload. A missing case there IS the JSON-fallback,
//     schema-reject, dead-letter defect this guardrail exists to catch.
//  2. a pg builder constructs the GENERATED type (e.g. tenancyv1.
//     ExternalEgressPolicyUpdated), calls proto.Marshal on it and stores the
//     bytes on outbox.Row.Payload. dispatcher.go publishes row.Payload
//     VERBATIM, so the wire bytes are already correct binary protobuf and no
//     encoder case is reachable, let alone required.
//
// Without this skip, path 2 reported a missing encoder for a correctly encoded
// topic (chora.tenancy.external_egress_policy.updated.v1) and the guardrail was
// red on a defect that did not exist. "Fixing" that by adding a MarshalPayload
// case would be worse than leaving it: dead code that is never called, turning
// a visible false alarm into invisible false confidence.
//
// Deliberately keyed on the MECHANISM rather than an allowlist of topics. An
// allowlist rots the moment a producer changes shape; this reads the two things
// that make a file path-2 by construction. TestFabricGuardrail_PreMarshalSkipIs
// Precise pins that it excludes only the pre-marshalling builders and leaves
// every map-path producer scanned.
func preMarshalsItsPayload(src string) bool {
	return strings.Contains(src, "proto.Marshal(") && strings.Contains(src, "outbox.Row{")
}

// producedTenancyTopics scans chora-tenancy's Go source for tenancy topic
// string literals, EXCLUDING the encoder (protomarshal — the code under test),
// the decoder (protodecode) and the subscribers (consume-only surfaces) and
// test files. What remains is the set of topics the service actually emits.
func producedTenancyTopics(t *testing.T, root string) map[string]bool {
	t.Helper()
	svcRoot := filepath.Join(root, "services", "chora-tenancy")
	produced := map[string]bool{}
	err := filepath.WalkDir(svcRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// Consume-only + self surfaces do not count as producers.
		if strings.Contains(path, string(filepath.Separator)+"protomarshal"+string(filepath.Separator)) ||
			strings.Contains(path, string(filepath.Separator)+"protodecode"+string(filepath.Separator)) ||
			strings.Contains(path, string(filepath.Separator)+"subscribers"+string(filepath.Separator)) ||
			strings.HasSuffix(path, "_consumer.go") ||
			strings.HasSuffix(path, "_subscriber.go") {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		// A file that marshals its own payload is not an ENCODER producer.
		if preMarshalsItsPayload(string(raw)) {
			return nil
		}
		for topic := range topicLiterals(string(raw)) {
			produced[topic] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk chora-tenancy source: %v", err)
	}
	if len(produced) == 0 {
		t.Fatal("0 produced tenancy topics found — source scan is broken (would make the guardrail vacuously green)")
	}
	return produced
}

// TestFabricGuardrail_EveryProducedBinaryTopicHasEncoder is the Flag-1 gate.
// For every tenancy topic that chora-tenancy PRODUCES and that is bound to a
// binary Schema Registry schema, MarshalPayload MUST route to a real encoder
// (not ErrUnsupportedTopic → JSON fallback → schema-reject → dead-letter).
func TestFabricGuardrail_EveryProducedBinaryTopicHasEncoder(t *testing.T) {
	root, ok := repoRoot(t)
	if !ok {
		t.Skip("monorepo root (chora-infra + services/chora-tenancy) not found — not reachable from this split checkout")
	}
	binaryBound := tenancyBinaryBoundTopics(t, root)
	produced := producedTenancyTopics(t, root)
	env := fixedEnvelope()
	// A minimal payload with the common id keys the encoders reach for; absent
	// fields are proto3-default-omitted, so this is enough to prove routing.
	minimal := map[string]any{
		"course_id":     "c",
		"assessment_id": "a",
		"submission_id": "s",
		"oe_batch_id":   "b",
	}

	checked := 0
	for topic := range binaryBound {
		if !produced[topic] {
			// Declared-only scaffolding (schema provisioned, no producer yet).
			t.Logf("declared-only (binary schema, no chora-tenancy producer yet): %s", topic)
			continue
		}
		_, err := protomarshal.MarshalPayload(topic, env, minimal)
		if protomarshal.IsUnsupportedTopic(err) {
			t.Errorf("topic %q is PRODUCED by chora-tenancy and bound to a binary Pub/Sub "+
				"schema, but protomarshal has NO encoder case → JSON fallback → Schema "+
				"Registry rejects → dead-letter (kg_hexagon_fog class). Add a MarshalPayload case.", topic)
			continue
		}
		if err != nil {
			t.Errorf("topic %q: MarshalPayload returned a non-routing error: %v", topic, err)
			continue
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("guardrail asserted 0 produced binary-bound topics — the TF parse or the source scan is broken")
	}
	t.Logf("Flag-1 guardrail: %d produced binary-bound tenancy topics all have encoders", checked)
}

// The pre-marshal skip must be PRECISE, not a blanket suppression. It has to
// exclude the pg builders, whose bytes are already correct binary protobuf, and
// must NOT swallow the map-path producers, which are exactly where a missing
// encoder causes the JSON fallback this guardrail exists to catch. Asserted
// against the real files rather than synthetic snippets, so the rule is pinned
// to the code it actually governs.
func TestFabricGuardrail_PreMarshalSkipIsPrecise(t *testing.T) {
	root, ok := repoRoot(t)
	if !ok {
		t.Skip("monorepo root (chora-infra + services/chora-tenancy) not found — not reachable from this split checkout")
	}
	svc := filepath.Join(root, "services", "chora-tenancy")

	read := func(rel string) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(svc, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		return string(raw)
	}

	// Path 2: builds the generated type, proto.Marshal's it onto outbox.Row.
	for _, rel := range []string{
		filepath.Join("internal", "adapter", "pg", "external_egress_outbox.go"),
		filepath.Join("internal", "adapter", "pg", "bootstrap_outbox.go"),
	} {
		if !preMarshalsItsPayload(read(rel)) {
			t.Errorf("%s marshals its own payload onto outbox.Row and MUST be skipped; "+
				"scanning it reports a missing encoder for a correctly encoded topic", rel)
		}
	}

	// Path 1: hands a payload MAP to the publisher, so a missing encoder is real.
	for _, rel := range []string{
		filepath.Join("internal", "adapter", "http", "v2_handlers.go"),
		filepath.Join("internal", "adapter", "billingwebhook", "handler.go"),
		filepath.Join("internal", "adapter", "skillsfuture", "skillsfuture.go"),
	} {
		if preMarshalsItsPayload(read(rel)) {
			t.Errorf("%s publishes through the MAP path and must stay SCANNED; "+
				"skipping it would hide a genuine missing-encoder gap", rel)
		}
	}
}
