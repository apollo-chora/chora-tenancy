package billingwebhook

import (
	"os"
	"strings"
)

// resolveProject reports the GCP project this service runs in, for the
// service-index payload.
//
// It was the literal "chora-489812" until 2026-09-02 (CHO-2419). No manifest
// could reach it, so a second org would serve THIS org's project id from its
// own index endpoint. On the O+ governance surface that is more than cosmetic.
//
// CHORA_SOURCE_PROJECT is read first because it is the var the event
// envelope's source_project is stamped from, so the index agrees with the
// events by construction. GOOGLE_CLOUD_PROJECT is tried second because every
// service manifest already sets it, which means this reads correctly in a
// second org even before the first var is set anywhere. The literal stays last
// so this estate's behaviour is provably unchanged.
func resolveProject() string {
	for _, k := range []string{"CHORA_SOURCE_PROJECT", "GOOGLE_CLOUD_PROJECT"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return "chora-489812"
}
