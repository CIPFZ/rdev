# rdev broker reference

With `RDEV_BROKER_SOCKET`, use only operations advertised by the authenticated
broker. Fleet mutations require a plan, digest-bound approval and execution.
Never fall back to direct SSH when a broker route is unsupported. Keep client,
project and principal identifiers explicit and do not expose another owner's
configuration or administrator policy paths.
