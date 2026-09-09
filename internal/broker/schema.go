package broker

// These constants are shared by the durable readers, writers and compatibility
// discovery. Each format can evolve independently; do not merge their versions.
const (
	JobRegistrySchemaVersion     = 1
	MutationSchemaVersion        = 1
	JobEventSchemaVersion        = 1
	SecretSchemaVersion          = 1
	AuditSchemaVersion           = 1
	AuditContinuitySchemaVersion = 1
)
