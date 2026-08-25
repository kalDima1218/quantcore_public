package finam

import "QuantCore/modlog"

// mlog is the finam module's log: every broker-facing error/warning below still goes
// to stderr and is also appended to logs/finam.log.
var mlog = modlog.For("finam")
