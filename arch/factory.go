package arch

// Factories Is a list of factories used by `Gluon` internals (e.g. ID factory) to successfully execute certain
// operations.
//
// For example, when Gluon is constructing a message when a publication was requested, Gluon internals use the
// specified ID factory to generate IDs which contains the underlying unique identification algorithm (e.g. UUID).
type Factories struct {
	IDFactory IDFactory
}

// IDFactory Is the kind of factory for generating unique identifications using a specific algorithm (e.g. UUID, Nano ID)
//
// The default IDFactory is FactoryUUID (uses google/uuid package).
type IDFactory interface {
	NewID() (string, error)
}

var defaultIDFactory IDFactory = FactoryUUID{}
