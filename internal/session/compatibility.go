package session

// TrustSchemaVersion is the exact version accepted by the trust-store reader.
const TrustSchemaVersion = trustFileVersion

// HostConfigShape exposes types only, never configured values. The format is
// currently unversioned; a version number here would imply a validation rule
// that the legacy registry reader does not implement.
func HostConfigShape() any { return hostFile{} }
