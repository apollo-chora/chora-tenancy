package main

// grpcPortFromEnv resolves the gRPC listen port.
//
// CHORA_GRPC_PORT is the canonical platform variable (it is namespaced and
// scales in a compose file with dozens of services). GRPC_PORT is accepted as
// a temporary compatibility alias for deployments that predate the convention.
// Only CHORA_GRPC_PORT is documented and set by the root compose; the alias is
// removed once every repo has migrated.
func grpcPortFromEnv() string {
	return envOrDefault("CHORA_GRPC_PORT", envOrDefault("GRPC_PORT", "9090"))
}
