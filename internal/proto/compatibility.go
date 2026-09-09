package proto

import "sort"

// ErrorContractVersion changes when an existing code or its retry semantics
// changes. Adding a code still requires feature/version gating for older peers:
// ErrorEnvelope.Validate deliberately rejects unknown codes.
const ErrorContractVersion = 1

// TypedProtocolVersion is the first version that preserves typed terminal and
// caller identity semantics. Shared broker requests cannot use the v2 fallback.
const TypedProtocolVersion = 3

// ErrorDescriptors returns the registry used by NewError and Validate, in a
// stable order. Consumers must not maintain a second list of wire error codes.
func ErrorDescriptors() []ErrorDescriptor {
	out := make([]ErrorDescriptor, 0, len(errorRegistry))
	for _, descriptor := range errorRegistry {
		out = append(out, descriptor)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}
